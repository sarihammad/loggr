#!/usr/bin/env python3
"""
Kafka consumer for reading logs and forwarding them for processing.
"""
import sys
import time
import argparse
from typing import Dict, Any, Callable, Optional

from kafka import KafkaConsumer

sys.path.append(".")
from config import settings
from config.utils import deserialize_log, get_logger


class LogConsumer:
    """
    Consumes logs from Kafka and forwards them for processing.
    """

    def __init__(
        self,
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = settings.KAFKA_CONSUMER_GROUP,
        topic: str = settings.KAFKA_TOPICS["raw_logs"],
        auto_offset_reset: str = "latest",
    ):
        """
        Initialize the log consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            topic: Kafka topic to consume from
            auto_offset_reset: Auto offset reset strategy
        """
        self.logger = get_logger(service="log-consumer")
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda x: deserialize_log(x.decode("utf-8")),
        )
        self.logger.info(
            "Initialized Kafka consumer",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic=topic,
        )

    def consume_logs(
        self,
        processor: Optional[Callable[[Dict[str, Any]], None]] = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 1000,
    ) -> None:
        """
        Consume logs from Kafka and process them.
        
        Args:
            processor: Function to process each log (optional)
            batch_size: Maximum number of logs to process in a batch
            batch_timeout_ms: Maximum time to wait for a batch in milliseconds
        """
        self.logger.info(
            "Starting log consumption",
            batch_size=batch_size,
            batch_timeout_ms=batch_timeout_ms,
        )
        
        try:
            batch = []
            last_process_time = time.time()
            
            for message in self.consumer:
                log_data = message.value
                
                # Add the log to the batch
                batch.append(log_data)
                
                # Process the batch if it's full or if the timeout has elapsed
                current_time = time.time()
                batch_timeout_elapsed = (current_time - last_process_time) * 1000 >= batch_timeout_ms
                
                if len(batch) >= batch_size or batch_timeout_elapsed:
                    self._process_batch(batch, processor)
                    batch = []
                    last_process_time = current_time
                    
        except KeyboardInterrupt:
            self.logger.info("Log consumption stopped by user")
        finally:
            # Process any remaining logs
            if batch:
                self._process_batch(batch, processor)
                
            self.consumer.close()
            self.logger.info("Kafka consumer closed")

    def _process_batch(
        self,
        batch: list[Dict[str, Any]],
        processor: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        """
        Process a batch of logs.
        
        Args:
            batch: Batch of logs to process
            processor: Function to process each log (optional)
        """
        if not batch:
            return
            
        self.logger.debug(f"Processing batch of {len(batch)} logs")
        
        # Process each log
        for log_data in batch:
            try:
                if processor:
                    processor(log_data)
                else:
                    # Default processing: just print the log
                    service = log_data.get("service", "unknown")
                    log_level = log_data.get("log_level", "INFO")
                    message = log_data.get("message", "")
                    request_id = log_data.get("request_id", "")
                    
                    self.logger.info(
                        f"[{service}] {message}",
                        log_level=log_level,
                        request_id=request_id,
                    )
            except Exception as e:
                self.logger.error(
                    "Error processing log",
                    error=str(e),
                    log_data=log_data,
                )
                
        self.logger.debug(f"Processed batch of {len(batch)} logs")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Consume logs from Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=settings.KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=settings.KAFKA_CONSUMER_GROUP,
        help="Consumer group ID",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=settings.KAFKA_TOPICS["raw_logs"],
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--auto-offset-reset",
        type=str,
        choices=["earliest", "latest"],
        default="latest",
        help="Auto offset reset strategy",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Maximum number of logs to process in a batch",
    )
    parser.add_argument(
        "--batch-timeout-ms",
        type=int,
        default=1000,
        help="Maximum time to wait for a batch in milliseconds",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    consumer = LogConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topic=args.topic,
        auto_offset_reset=args.auto_offset_reset,
    )
    consumer.consume_logs(
        processor=None,  # Use default processor
        batch_size=args.batch_size,
        batch_timeout_ms=args.batch_timeout_ms,
    ) 