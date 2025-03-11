#!/usr/bin/env python3
"""
S3 uploader for archiving raw logs.
"""
import sys
import os
import time
import json
import datetime
import argparse
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

sys.path.append(".")
from config import settings
from config.utils import get_logger, parse_timestamp


class S3Uploader:
    """
    Uploads logs to S3 for long-term storage.
    """

    def __init__(
        self,
        bucket_name: str = settings.S3_BUCKET_NAME,
        prefix: str = settings.S3_PREFIX,
        region: str = settings.AWS_REGION,
    ):
        """
        Initialize the S3 uploader.
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 key prefix
            region: AWS region
        """
        self.logger = get_logger(service="s3-uploader")
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.region = region
        
        # Initialize S3 client
        self.s3 = boto3.client("s3", region_name=region)
        
        # Ensure the bucket exists
        self._ensure_bucket_exists()
        
        self.logger.info(
            "Initialized S3 uploader",
            bucket_name=bucket_name,
            prefix=prefix,
            region=region,
        )

    def _ensure_bucket_exists(self) -> None:
        """
        Ensure the S3 bucket exists, creating it if necessary.
        """
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"S3 bucket {self.bucket_name} exists")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                self.logger.info(f"Creating S3 bucket {self.bucket_name}")
                try:
                    if self.region == "us-east-1":
                        self.s3.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": self.region},
                        )
                    
                    # Configure lifecycle policy for log expiration
                    self._configure_lifecycle_policy()
                except ClientError as create_error:
                    self.logger.error(
                        f"Failed to create S3 bucket: {str(create_error)}",
                        bucket_name=self.bucket_name,
                    )
                    raise
            else:
                self.logger.error(
                    f"Error checking S3 bucket: {str(e)}",
                    bucket_name=self.bucket_name,
                )
                raise

    def _configure_lifecycle_policy(self) -> None:
        """
        Configure lifecycle policy for log expiration.
        """
        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "ExpireLogs",
                            "Status": "Enabled",
                            "Prefix": self.prefix,
                            "Expiration": {"Days": settings.S3_RETENTION_DAYS},
                        }
                    ]
                },
            )
            self.logger.info(
                f"Configured lifecycle policy for S3 bucket {self.bucket_name}",
                retention_days=settings.S3_RETENTION_DAYS,
            )
        except ClientError as e:
            self.logger.error(
                f"Failed to configure lifecycle policy: {str(e)}",
                bucket_name=self.bucket_name,
            )

    def _generate_s3_key(self, log_data: Dict[str, Any]) -> str:
        """
        Generate an S3 key for a log.
        
        Args:
            log_data: The log data
            
        Returns:
            The S3 key
        """
        # Parse the timestamp
        timestamp_str = log_data.get("timestamp")
        if not timestamp_str:
            # Use current time if timestamp is missing
            dt = datetime.datetime.now(datetime.timezone.utc)
        else:
            try:
                dt = parse_timestamp(timestamp_str)
            except ValueError:
                # Use current time if timestamp is invalid
                dt = datetime.datetime.now(datetime.timezone.utc)
                
        # Format: logs/service/YYYY/MM/DD/HH/service-YYYYMMDD-HHMMSS-UUID.json
        service = log_data.get("service", "unknown")
        request_id = log_data.get("request_id", "")
        date_part = dt.strftime("%Y/%m/%d/%H")
        file_part = dt.strftime("%Y%m%d-%H%M%S")
        
        return f"{self.prefix}/{service}/{date_part}/{service}-{file_part}-{request_id}.json"

    def upload_log(self, log_data: Dict[str, Any]) -> str:
        """
        Upload a log to S3.
        
        Args:
            log_data: The log data
            
        Returns:
            The S3 key
        """
        s3_key = self._generate_s3_key(log_data)
        
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(log_data),
                ContentType="application/json",
            )
            self.logger.debug(
                "Uploaded log to S3",
                bucket=self.bucket_name,
                key=s3_key,
                service=log_data.get("service"),
                request_id=log_data.get("request_id"),
            )
            return s3_key
        except ClientError as e:
            self.logger.error(
                f"Failed to upload log to S3: {str(e)}",
                bucket=self.bucket_name,
                key=s3_key,
            )
            raise

    def upload_logs(self, logs: List[Dict[str, Any]]) -> List[str]:
        """
        Upload multiple logs to S3.
        
        Args:
            logs: The logs to upload
            
        Returns:
            The S3 keys
        """
        s3_keys = []
        
        for log_data in logs:
            try:
                s3_key = self.upload_log(log_data)
                s3_keys.append(s3_key)
            except Exception as e:
                self.logger.error(
                    f"Error uploading log: {str(e)}",
                    service=log_data.get("service"),
                    request_id=log_data.get("request_id"),
                )
                
        return s3_keys

    def download_log(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Download a log from S3.
        
        Args:
            s3_key: The S3 key
            
        Returns:
            The log data, or None if not found
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            log_data = json.loads(response["Body"].read().decode("utf-8"))
            self.logger.debug(
                "Downloaded log from S3",
                bucket=self.bucket_name,
                key=s3_key,
            )
            return log_data
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.warning(
                    f"Log not found in S3: {s3_key}",
                    bucket=self.bucket_name,
                )
                return None
            else:
                self.logger.error(
                    f"Failed to download log from S3: {str(e)}",
                    bucket=self.bucket_name,
                    key=s3_key,
                )
                raise

    def list_logs(
        self,
        service: Optional[str] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        max_keys: int = 1000,
    ) -> List[str]:
        """
        List logs in S3.
        
        Args:
            service: Filter by service (optional)
            start_date: Filter by start date (optional)
            end_date: Filter by end date (optional)
            max_keys: Maximum number of keys to return
            
        Returns:
            List of S3 keys
        """
        prefix = self.prefix
        
        if service:
            prefix = f"{prefix}/{service}"
            
        if start_date:
            # Format: logs/service/YYYY/MM/DD
            date_prefix = start_date.strftime("%Y/%m/%d")
            prefix = f"{prefix}/{date_prefix}"
            
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys,
            )
            
            s3_keys = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        
                        # Filter by end date if specified
                        if end_date:
                            # Extract date from key
                            # Format: logs/service/YYYY/MM/DD/HH/service-YYYYMMDD-HHMMSS-UUID.json
                            try:
                                date_str = key.split("-")[1]  # YYYYMMDD
                                time_str = key.split("-")[2].split("-")[0]  # HHMMSS
                                dt = datetime.datetime.strptime(
                                    f"{date_str} {time_str}",
                                    "%Y%m%d %H%M%S",
                                ).replace(tzinfo=datetime.timezone.utc)
                                
                                if dt > end_date:
                                    continue
                            except (IndexError, ValueError):
                                # Skip if we can't parse the date
                                continue
                                
                        s3_keys.append(key)
                        
                        if len(s3_keys) >= max_keys:
                            break
                            
                if len(s3_keys) >= max_keys:
                    break
                    
            self.logger.info(
                f"Listed {len(s3_keys)} logs in S3",
                bucket=self.bucket_name,
                prefix=prefix,
                service=service,
            )
            return s3_keys
        except ClientError as e:
            self.logger.error(
                f"Failed to list logs in S3: {str(e)}",
                bucket=self.bucket_name,
                prefix=prefix,
            )
            raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Upload logs to S3")
    parser.add_argument(
        "--bucket",
        type=str,
        default=settings.S3_BUCKET_NAME,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=settings.S3_PREFIX,
        help="S3 key prefix",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=settings.AWS_REGION,
        help="AWS region",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List logs in S3",
    )
    parser.add_argument(
        "--service",
        type=str,
        help="Filter by service",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Filter by start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="Filter by end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max-keys",
        type=int,
        default=1000,
        help="Maximum number of keys to return",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    uploader = S3Uploader(
        bucket_name=args.bucket,
        prefix=args.prefix,
        region=args.region,
    )
    
    if args.list:
        # Parse dates
        start_date = None
        if args.start_date:
            start_date = datetime.datetime.strptime(
                args.start_date, "%Y-%m-%d"
            ).replace(tzinfo=datetime.timezone.utc)
            
        end_date = None
        if args.end_date:
            end_date = datetime.datetime.strptime(
                args.end_date, "%Y-%m-%d"
            ).replace(tzinfo=datetime.timezone.utc)
            # Set to end of day
            end_date = end_date.replace(hour=23, minute=59, second=59)
            
        # List logs
        s3_keys = uploader.list_logs(
            service=args.service,
            start_date=start_date,
            end_date=end_date,
            max_keys=args.max_keys,
        )
        
        # Print keys
        for key in s3_keys:
            print(key) 