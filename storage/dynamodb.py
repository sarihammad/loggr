#!/usr/bin/env python3
"""
DynamoDB module for storing aggregated log data.
"""
import sys
import time
import json
import datetime
import argparse
from typing import Dict, Any, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

sys.path.append(".")
from config import settings
from config.utils import get_logger, parse_timestamp, current_timestamp


class DynamoDBStorage:
    """
    Stores aggregated log data in DynamoDB for real-time querying.
    """

    def __init__(
        self,
        table_name: str = settings.DYNAMODB_TABLE_NAME,
        region: str = settings.AWS_REGION,
    ):
        """
        Initialize the DynamoDB storage.
        
        Args:
            table_name: DynamoDB table name
            region: AWS region
        """
        self.logger = get_logger(service="dynamodb-storage")
        self.table_name = table_name
        self.region = region
        
        # Initialize DynamoDB resource
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)
        
        # Ensure the table exists
        self._ensure_table_exists()
        
        self.logger.info(
            "Initialized DynamoDB storage",
            table_name=table_name,
            region=region,
        )

    def _ensure_table_exists(self) -> None:
        """
        Ensure the DynamoDB table exists, creating it if necessary.
        """
        try:
            self.table.table_status
            self.logger.info(f"DynamoDB table {self.table_name} exists")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                self.logger.info(f"Creating DynamoDB table {self.table_name}")
                try:
                    table = self.dynamodb.create_table(
                        TableName=self.table_name,
                        KeySchema=[
                            {"AttributeName": "service", "KeyType": "HASH"},  # Partition key
                            {"AttributeName": "timestamp", "KeyType": "RANGE"},  # Sort key
                        ],
                        AttributeDefinitions=[
                            {"AttributeName": "service", "AttributeType": "S"},
                            {"AttributeName": "timestamp", "AttributeType": "S"},
                            {"AttributeName": "log_level", "AttributeType": "S"},
                        ],
                        GlobalSecondaryIndexes=[
                            {
                                "IndexName": "LogLevelIndex",
                                "KeySchema": [
                                    {"AttributeName": "log_level", "KeyType": "HASH"},
                                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                                ],
                                "Projection": {"ProjectionType": "ALL"},
                                "ProvisionedThroughput": {
                                    "ReadCapacityUnits": settings.DYNAMODB_THROUGHPUT["ReadCapacityUnits"],
                                    "WriteCapacityUnits": settings.DYNAMODB_THROUGHPUT["WriteCapacityUnits"],
                                },
                            },
                        ],
                        ProvisionedThroughput={
                            "ReadCapacityUnits": settings.DYNAMODB_THROUGHPUT["ReadCapacityUnits"],
                            "WriteCapacityUnits": settings.DYNAMODB_THROUGHPUT["WriteCapacityUnits"],
                        },
                    )
                    
                    # Wait for the table to be created
                    table.meta.client.get_waiter("table_exists").wait(TableName=self.table_name)
                    self.logger.info(f"Created DynamoDB table {self.table_name}")
                except ClientError as create_error:
                    self.logger.error(
                        f"Failed to create DynamoDB table: {str(create_error)}",
                        table_name=self.table_name,
                    )
                    raise
            else:
                self.logger.error(
                    f"Error checking DynamoDB table: {str(e)}",
                    table_name=self.table_name,
                )
                raise

    def store_log(self, log_data: Dict[str, Any]) -> None:
        """
        Store a log in DynamoDB.
        
        Args:
            log_data: The log data
        """
        try:
            # Ensure timestamp is in ISO format
            if "timestamp" not in log_data:
                log_data["timestamp"] = current_timestamp()
                
            # Store the log
            self.table.put_item(Item=log_data)
            
            self.logger.debug(
                "Stored log in DynamoDB",
                service=log_data.get("service"),
                log_level=log_data.get("log_level"),
                timestamp=log_data.get("timestamp"),
            )
        except ClientError as e:
            self.logger.error(
                f"Failed to store log in DynamoDB: {str(e)}",
                service=log_data.get("service"),
                log_level=log_data.get("log_level"),
            )
            raise

    def store_logs(self, logs: List[Dict[str, Any]]) -> None:
        """
        Store multiple logs in DynamoDB.
        
        Args:
            logs: The logs to store
        """
        with self.table.batch_writer() as batch:
            for log_data in logs:
                try:
                    # Ensure timestamp is in ISO format
                    if "timestamp" not in log_data:
                        log_data["timestamp"] = current_timestamp()
                        
                    # Store the log
                    batch.put_item(Item=log_data)
                except Exception as e:
                    self.logger.error(
                        f"Error storing log: {str(e)}",
                        service=log_data.get("service"),
                        log_level=log_data.get("log_level"),
                    )
                    
        self.logger.info(f"Stored {len(logs)} logs in DynamoDB")

    def store_aggregated_log(
        self,
        service: str,
        log_level: str,
        count: int,
        window_start: str,
        window_end: str,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Store an aggregated log in DynamoDB.
        
        Args:
            service: The service name
            log_level: The log level
            count: The count of logs
            window_start: The start of the time window
            window_end: The end of the time window
            additional_data: Additional data to include (optional)
        """
        try:
            # Create the aggregated log
            aggregated_log = {
                "service": service,
                "log_level": log_level,
                "timestamp": window_end,  # Use the end of the window as the timestamp
                "count": count,
                "window_start": window_start,
                "window_end": window_end,
                "type": "aggregated",  # Mark as an aggregated log
            }
            
            if additional_data:
                aggregated_log.update(additional_data)
                
            # Store the aggregated log
            self.table.put_item(Item=aggregated_log)
            
            self.logger.debug(
                "Stored aggregated log in DynamoDB",
                service=service,
                log_level=log_level,
                count=count,
                window_start=window_start,
                window_end=window_end,
            )
        except ClientError as e:
            self.logger.error(
                f"Failed to store aggregated log in DynamoDB: {str(e)}",
                service=service,
                log_level=log_level,
            )
            raise

    def query_logs_by_service(
        self,
        service: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        log_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query logs by service.
        
        Args:
            service: The service name
            start_time: The start time (optional)
            end_time: The end time (optional)
            log_level: The log level (optional)
            limit: The maximum number of logs to return
            
        Returns:
            The logs
        """
        try:
            # Build the key condition
            key_condition = Key("service").eq(service)
            
            if start_time and end_time:
                key_condition = key_condition & Key("timestamp").between(start_time, end_time)
            elif start_time:
                key_condition = key_condition & Key("timestamp").gte(start_time)
            elif end_time:
                key_condition = key_condition & Key("timestamp").lte(end_time)
                
            # Build the filter expression
            filter_expression = None
            if log_level:
                filter_expression = Attr("log_level").eq(log_level)
                
            # Query the table
            if filter_expression:
                response = self.table.query(
                    KeyConditionExpression=key_condition,
                    FilterExpression=filter_expression,
                    Limit=limit,
                )
            else:
                response = self.table.query(
                    KeyConditionExpression=key_condition,
                    Limit=limit,
                )
                
            logs = response.get("Items", [])
            
            self.logger.info(
                f"Queried {len(logs)} logs by service",
                service=service,
                start_time=start_time,
                end_time=end_time,
                log_level=log_level,
            )
            
            return logs
        except ClientError as e:
            self.logger.error(
                f"Failed to query logs by service: {str(e)}",
                service=service,
            )
            raise

    def query_logs_by_log_level(
        self,
        log_level: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        service: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query logs by log level.
        
        Args:
            log_level: The log level
            start_time: The start time (optional)
            end_time: The end time (optional)
            service: The service name (optional)
            limit: The maximum number of logs to return
            
        Returns:
            The logs
        """
        try:
            # Build the key condition
            key_condition = Key("log_level").eq(log_level)
            
            if start_time and end_time:
                key_condition = key_condition & Key("timestamp").between(start_time, end_time)
            elif start_time:
                key_condition = key_condition & Key("timestamp").gte(start_time)
            elif end_time:
                key_condition = key_condition & Key("timestamp").lte(end_time)
                
            # Build the filter expression
            filter_expression = None
            if service:
                filter_expression = Attr("service").eq(service)
                
            # Query the table
            if filter_expression:
                response = self.table.query(
                    IndexName="LogLevelIndex",
                    KeyConditionExpression=key_condition,
                    FilterExpression=filter_expression,
                    Limit=limit,
                )
            else:
                response = self.table.query(
                    IndexName="LogLevelIndex",
                    KeyConditionExpression=key_condition,
                    Limit=limit,
                )
                
            logs = response.get("Items", [])
            
            self.logger.info(
                f"Queried {len(logs)} logs by log level",
                log_level=log_level,
                start_time=start_time,
                end_time=end_time,
                service=service,
            )
            
            return logs
        except ClientError as e:
            self.logger.error(
                f"Failed to query logs by log level: {str(e)}",
                log_level=log_level,
            )
            raise

    def get_error_rate(
        self,
        service: str,
        window_minutes: int = 5,
    ) -> Tuple[float, int, int]:
        """
        Get the error rate for a service.
        
        Args:
            service: The service name
            window_minutes: The time window in minutes
            
        Returns:
            The error rate, total log count, and error count
        """
        try:
            # Calculate the start time
            end_time = datetime.datetime.now(datetime.timezone.utc)
            start_time = end_time - datetime.timedelta(minutes=window_minutes)
            
            # Query all logs for the service in the time window
            logs = self.query_logs_by_service(
                service=service,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                limit=1000,  # Increase limit to get a better sample
            )
            
            # Count the total logs and error logs
            total_count = len(logs)
            error_count = sum(1 for log in logs if log.get("log_level") in ["ERROR", "CRITICAL"])
            
            # Calculate the error rate
            error_rate = error_count / total_count if total_count > 0 else 0
            
            self.logger.info(
                f"Calculated error rate for service {service}",
                error_rate=error_rate,
                total_count=total_count,
                error_count=error_count,
                window_minutes=window_minutes,
            )
            
            return error_rate, total_count, error_count
        except ClientError as e:
            self.logger.error(
                f"Failed to get error rate: {str(e)}",
                service=service,
            )
            raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Query logs from DynamoDB")
    parser.add_argument(
        "--table",
        type=str,
        default=settings.DYNAMODB_TABLE_NAME,
        help="DynamoDB table name",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=settings.AWS_REGION,
        help="AWS region",
    )
    parser.add_argument(
        "--service",
        type=str,
        help="Filter by service",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=settings.LOG_LEVELS,
        help="Filter by log level",
    )
    parser.add_argument(
        "--start-time",
        type=str,
        help="Filter by start time (ISO 8601 format)",
    )
    parser.add_argument(
        "--end-time",
        type=str,
        help="Filter by end time (ISO 8601 format)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of logs to return",
    )
    parser.add_argument(
        "--error-rate",
        action="store_true",
        help="Calculate error rate for a service",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=5,
        help="Time window in minutes for error rate calculation",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    storage = DynamoDBStorage(
        table_name=args.table,
        region=args.region,
    )
    
    if args.error_rate and args.service:
        # Calculate error rate
        error_rate, total_count, error_count = storage.get_error_rate(
            service=args.service,
            window_minutes=args.window_minutes,
        )
        
        print(f"Error rate for {args.service}: {error_rate:.2%}")
        print(f"Total logs: {total_count}")
        print(f"Error logs: {error_count}")
        print(f"Time window: {args.window_minutes} minutes")
    elif args.service or args.log_level:
        # Query logs
        if args.log_level and not args.service:
            # Query by log level
            logs = storage.query_logs_by_log_level(
                log_level=args.log_level,
                start_time=args.start_time,
                end_time=args.end_time,
                service=args.service,
                limit=args.limit,
            )
        else:
            # Query by service
            logs = storage.query_logs_by_service(
                service=args.service,
                start_time=args.start_time,
                end_time=args.end_time,
                log_level=args.log_level,
                limit=args.limit,
            )
            
        # Print logs
        for log in logs:
            print(json.dumps(log, indent=2))
            print("---")
    else:
        print("Please specify a service or log level to query.")
        parser.print_help() 