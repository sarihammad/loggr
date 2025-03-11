#!/usr/bin/env python3
"""
Apache Flink job for real-time log processing.
"""
import sys
import os
import json
import datetime
import argparse
from typing import Dict, Any, List, Tuple, Optional, Iterator

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner

sys.path.append(".")
from config import settings
from config.utils import (
    get_logger,
    deserialize_log,
    serialize_log,
    parse_timestamp,
    current_timestamp,
)
from flink.anomaly_detection import AnomalyDetector
from storage.dynamodb import DynamoDBStorage
from storage.s3_uploader import S3Uploader
from alerts.sns_publisher import SNSPublisher


class LogTimestampAssigner(TimestampAssigner):
    """
    Assigns timestamps to log events based on the log timestamp.
    """

    def extract_timestamp(self, log_str: str, record_timestamp: int) -> int:
        """
        Extract the timestamp from a log event.
        
        Args:
            log_str: The log event as a JSON string
            record_timestamp: The record timestamp
            
        Returns:
            The extracted timestamp in milliseconds
        """
        try:
            log_data = json.loads(log_str)
            timestamp_str = log_data.get("timestamp")
            if timestamp_str:
                dt = parse_timestamp(timestamp_str)
                return int(dt.timestamp() * 1000)
        except (json.JSONDecodeError, ValueError):
            pass
            
        # Fall back to the record timestamp
        return record_timestamp


class LogDeserializationFunction(MapFunction):
    """
    Deserializes log events from JSON strings.
    """

    def map(self, log_str: str) -> Dict[str, Any]:
        """
        Deserialize a log event.
        
        Args:
            log_str: The log event as a JSON string
            
        Returns:
            The deserialized log event
        """
        return deserialize_log(log_str)


class LogAggregationFunction(KeyedProcessFunction):
    """
    Aggregates logs by service and log level.
    """

    def __init__(
        self,
        window_size_minutes: int = settings.ANOMALY_DETECTION["window_size_minutes"],
    ):
        """
        Initialize the log aggregation function.
        
        Args:
            window_size_minutes: The size of the time window in minutes
        """
        self.window_size_minutes = window_size_minutes
        self.window_size_ms = window_size_minutes * 60 * 1000
        self.state = {}
        self.logger = get_logger(service="log-aggregation")
        
        # Initialize the anomaly detector
        self.anomaly_detector = AnomalyDetector()
        
        # Initialize the DynamoDB storage
        self.dynamodb = DynamoDBStorage()
        
        # Initialize the S3 uploader
        self.s3_uploader = S3Uploader()
        
        # Initialize the SNS publisher
        self.sns_publisher = SNSPublisher()

    def process_element(
        self,
        log_data: Dict[str, Any],
        ctx: KeyedProcessFunction.Context,
        out: KeyedProcessFunction.Collector,
    ) -> None:
        """
        Process a log event.
        
        Args:
            log_data: The log event
            ctx: The context
            out: The collector
        """
        # Get the service and log level
        service = log_data.get("service", "unknown")
        log_level = log_data.get("log_level", "INFO")
        
        # Get the timestamp
        timestamp_str = log_data.get("timestamp")
        if not timestamp_str:
            timestamp_str = current_timestamp()
            log_data["timestamp"] = timestamp_str
            
        # Parse the timestamp
        try:
            timestamp = parse_timestamp(timestamp_str)
            timestamp_ms = int(timestamp.timestamp() * 1000)
        except ValueError:
            # Use the current time if the timestamp is invalid
            timestamp = datetime.datetime.now(datetime.timezone.utc)
            timestamp_ms = int(timestamp.timestamp() * 1000)
            timestamp_str = timestamp.isoformat()
            log_data["timestamp"] = timestamp_str
            
        # Store the log in DynamoDB
        try:
            self.dynamodb.store_log(log_data)
        except Exception as e:
            self.logger.error(
                f"Failed to store log in DynamoDB: {str(e)}",
                service=service,
                log_level=log_level,
            )
            
        # Store the log in S3
        try:
            self.s3_uploader.upload_log(log_data)
        except Exception as e:
            self.logger.error(
                f"Failed to upload log to S3: {str(e)}",
                service=service,
                log_level=log_level,
            )
            
        # Initialize the state for the service if needed
        if service not in self.state:
            self.state[service] = {
                "logs": [],
                "last_window_end": 0,
            }
            
        # Add the log to the state
        self.state[service]["logs"].append(log_data)
        
        # Check if we need to process the window
        current_window_end = (timestamp_ms // self.window_size_ms) * self.window_size_ms + self.window_size_ms
        if current_window_end > self.state[service]["last_window_end"]:
            # Process the window
            self._process_window(service, current_window_end)
            
            # Update the last window end
            self.state[service]["last_window_end"] = current_window_end
            
        # Forward the log
        out.collect(log_data)

    def _process_window(self, service: str, window_end_ms: int) -> None:
        """
        Process a time window for a service.
        
        Args:
            service: The service name
            window_end_ms: The end of the time window in milliseconds
        """
        # Get the logs for the service
        logs = self.state[service]["logs"]
        
        # Filter logs for the current window
        window_start_ms = window_end_ms - self.window_size_ms
        window_logs = [
            log for log in logs
            if window_start_ms <= int(parse_timestamp(log["timestamp"]).timestamp() * 1000) < window_end_ms
        ]
        
        # Remove processed logs from the state
        self.state[service]["logs"] = [
            log for log in logs
            if int(parse_timestamp(log["timestamp"]).timestamp() * 1000) >= window_end_ms
        ]
        
        # Skip if there are no logs
        if not window_logs:
            return
            
        # Convert timestamps to ISO format
        window_start = datetime.datetime.fromtimestamp(
            window_start_ms / 1000, datetime.timezone.utc
        ).isoformat()
        window_end = datetime.datetime.fromtimestamp(
            window_end_ms / 1000, datetime.timezone.utc
        ).isoformat()
        
        # Count logs by log level
        log_level_counts = {level: 0 for level in settings.LOG_LEVELS}
        for log in window_logs:
            log_level = log.get("log_level", "INFO")
            log_level_counts[log_level] += 1
            
        # Store aggregated logs in DynamoDB
        for log_level, count in log_level_counts.items():
            if count > 0:
                try:
                    self.dynamodb.store_aggregated_log(
                        service=service,
                        log_level=log_level,
                        count=count,
                        window_start=window_start,
                        window_end=window_end,
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to store aggregated log in DynamoDB: {str(e)}",
                        service=service,
                        log_level=log_level,
                    )
                    
        # Detect anomalies
        try:
            anomalies = self.anomaly_detector.detect_anomalies(
                logs=window_logs,
                window_start=window_start,
                window_end=window_end,
            )
            
            # Publish alerts for anomalies
            for anomaly in anomalies:
                try:
                    self.sns_publisher.publish_alert(anomaly)
                except Exception as e:
                    self.logger.error(
                        f"Failed to publish alert: {str(e)}",
                        service=service,
                        anomaly_type=anomaly.get("anomaly_type"),
                    )
        except Exception as e:
            self.logger.error(
                f"Failed to detect anomalies: {str(e)}",
                service=service,
            )


def create_flink_job(
    bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
    input_topic: str = settings.KAFKA_TOPICS["raw_logs"],
    output_topic: str = settings.KAFKA_TOPICS["processed_logs"],
    consumer_group: str = settings.KAFKA_CONSUMER_GROUP,
    parallelism: int = settings.FLINK_PARALLELISM,
    checkpoint_interval: int = settings.FLINK_CHECKPOINT_INTERVAL,
    state_backend: str = settings.FLINK_STATE_BACKEND,
    checkpoint_dir: str = settings.FLINK_CHECKPOINT_DIR,
) -> None:
    """
    Create and execute the Flink job.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        input_topic: Kafka input topic
        output_topic: Kafka output topic
        consumer_group: Kafka consumer group
        parallelism: Flink parallelism
        checkpoint_interval: Checkpoint interval in milliseconds
        state_backend: State backend
        checkpoint_dir: Checkpoint directory
    """
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    env.enable_checkpointing(checkpoint_interval)
    
    # Set the state backend
    if state_backend == "filesystem":
        env.get_checkpoint_config().set_checkpoint_storage_dir(f"file://{checkpoint_dir}")
        
    # Set the time characteristic
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Create the Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics=input_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
        },
    )
    
    # Set the watermark strategy
    kafka_consumer.set_start_from_latest()
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(5))
        .with_timestamp_assigner(LogTimestampAssigner())
    )
    kafka_consumer = kafka_consumer.assign_timestamps_and_watermarks(watermark_strategy)
    
    # Create the Kafka producer
    kafka_producer = FlinkKafkaProducer(
        topic=output_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": bootstrap_servers,
        },
    )
    
    # Create the data stream
    stream = env.add_source(kafka_consumer)
    
    # Deserialize the logs
    deserialized_stream = stream.map(
        LogDeserializationFunction(),
        output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()),
    )
    
    # Key the stream by service
    keyed_stream = deserialized_stream.key_by(
        lambda log: log.get("service", "unknown"),
        key_type=Types.STRING(),
    )
    
    # Process the logs
    processed_stream = keyed_stream.process(
        LogAggregationFunction(),
        output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()),
    )
    
    # Serialize the logs
    serialized_stream = processed_stream.map(
        lambda log: serialize_log(log),
        output_type=Types.STRING(),
    )
    
    # Add the sink
    serialized_stream.add_sink(kafka_producer)
    
    # Execute the job
    env.execute("Log Processing Job")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the Flink job")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=settings.KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--input-topic",
        type=str,
        default=settings.KAFKA_TOPICS["raw_logs"],
        help="Kafka input topic",
    )
    parser.add_argument(
        "--output-topic",
        type=str,
        default=settings.KAFKA_TOPICS["processed_logs"],
        help="Kafka output topic",
    )
    parser.add_argument(
        "--consumer-group",
        type=str,
        default=settings.KAFKA_CONSUMER_GROUP,
        help="Kafka consumer group",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=settings.FLINK_PARALLELISM,
        help="Flink parallelism",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=settings.FLINK_CHECKPOINT_INTERVAL,
        help="Checkpoint interval in milliseconds",
    )
    parser.add_argument(
        "--state-backend",
        type=str,
        default=settings.FLINK_STATE_BACKEND,
        help="State backend",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=settings.FLINK_CHECKPOINT_DIR,
        help="Checkpoint directory",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    create_flink_job(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        consumer_group=args.consumer_group,
        parallelism=args.parallelism,
        checkpoint_interval=args.checkpoint_interval,
        state_backend=args.state_backend,
        checkpoint_dir=args.checkpoint_dir,
    ) 