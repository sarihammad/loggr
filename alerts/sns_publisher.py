#!/usr/bin/env python3
"""
SNS publisher for sending alerts.
"""
import sys
import json
import argparse
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

sys.path.append(".")
from config import settings
from config.utils import get_logger, serialize_log


class SNSPublisher:
    """
    Publishes alerts to AWS SNS.
    """

    def __init__(
        self,
        topic_arn: Optional[str] = settings.SNS_TOPIC_ARN,
        topic_name: str = settings.SNS_TOPIC_NAME,
        region: str = settings.AWS_REGION,
    ):
        """
        Initialize the SNS publisher.
        
        Args:
            topic_arn: SNS topic ARN (optional)
            topic_name: SNS topic name
            region: AWS region
        """
        self.logger = get_logger(service="sns-publisher")
        self.topic_arn = topic_arn
        self.topic_name = topic_name
        self.region = region
        
        # Initialize SNS client
        self.sns = boto3.client("sns", region_name=region)
        
        # Ensure the topic exists
        self._ensure_topic_exists()
        
        self.logger.info(
            "Initialized SNS publisher",
            topic_arn=self.topic_arn,
            topic_name=topic_name,
            region=region,
        )

    def _ensure_topic_exists(self) -> None:
        """
        Ensure the SNS topic exists, creating it if necessary.
        """
        if self.topic_arn:
            try:
                self.sns.get_topic_attributes(TopicArn=self.topic_arn)
                self.logger.info(f"SNS topic {self.topic_arn} exists")
                return
            except ClientError as e:
                if e.response["Error"]["Code"] != "NotFound":
                    self.logger.error(
                        f"Error checking SNS topic: {str(e)}",
                        topic_arn=self.topic_arn,
                    )
                    raise
                    
        # Topic doesn't exist or ARN not provided, create it
        try:
            response = self.sns.create_topic(Name=self.topic_name)
            self.topic_arn = response["TopicArn"]
            self.logger.info(f"Created SNS topic {self.topic_arn}")
        except ClientError as e:
            self.logger.error(
                f"Failed to create SNS topic: {str(e)}",
                topic_name=self.topic_name,
            )
            raise

    def publish_alert(self, alert_data: Dict[str, Any]) -> str:
        """
        Publish an alert to SNS.
        
        Args:
            alert_data: The alert data
            
        Returns:
            The message ID
        """
        try:
            # Format the alert message
            service = alert_data.get("service", "unknown")
            anomaly_type = alert_data.get("anomaly_type", "unknown")
            
            # Create a subject based on the anomaly type
            if anomaly_type == "error_rate":
                subject = f"Error Rate Anomaly Detected for {service}"
                message = (
                    f"Error rate of {alert_data.get('error_rate', 0):.2%} exceeds threshold "
                    f"of {alert_data.get('threshold', 0):.2%}.\n"
                    f"Error count: {alert_data.get('error_count', 0)}\n"
                    f"Total count: {alert_data.get('total_count', 0)}\n"
                    f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
                )
            elif anomaly_type == "volume":
                subject = f"Log Volume Anomaly Detected for {service}"
                message = (
                    f"Log count of {alert_data.get('log_count', 0)} is {alert_data.get('z_score', 0):.2f} "
                    f"standard deviations from the mean of {alert_data.get('mean', 0):.2f}.\n"
                    f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
                )
            elif anomaly_type == "pattern":
                subject = f"Log Pattern Anomaly Detected for {service}"
                message = (
                    f"Detected {alert_data.get('anomaly_count', 0)} anomalous logs out of "
                    f"{alert_data.get('total_count', 0)} logs ({alert_data.get('anomaly_ratio', 0):.2%}).\n"
                    f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
                )
            else:
                subject = f"Anomaly Detected for {service}"
                message = f"An anomaly was detected for service {service}.\n"
                message += f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
                
            # Add the full alert data as JSON
            message += f"\n\nFull alert data:\n{json.dumps(alert_data, indent=2)}"
            
            # Publish the message
            response = self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message,
            )
            
            message_id = response["MessageId"]
            
            self.logger.info(
                f"Published alert to SNS",
                topic_arn=self.topic_arn,
                message_id=message_id,
                service=service,
                anomaly_type=anomaly_type,
            )
            
            return message_id
        except ClientError as e:
            self.logger.error(
                f"Failed to publish alert to SNS: {str(e)}",
                topic_arn=self.topic_arn,
                service=alert_data.get("service"),
                anomaly_type=alert_data.get("anomaly_type"),
            )
            raise

    def subscribe_email(self, email: str) -> str:
        """
        Subscribe an email address to the SNS topic.
        
        Args:
            email: The email address
            
        Returns:
            The subscription ARN
        """
        try:
            response = self.sns.subscribe(
                TopicArn=self.topic_arn,
                Protocol="email",
                Endpoint=email,
            )
            
            subscription_arn = response["SubscriptionArn"]
            
            self.logger.info(
                f"Subscribed email to SNS topic",
                topic_arn=self.topic_arn,
                email=email,
                subscription_arn=subscription_arn,
            )
            
            return subscription_arn
        except ClientError as e:
            self.logger.error(
                f"Failed to subscribe email to SNS topic: {str(e)}",
                topic_arn=self.topic_arn,
                email=email,
            )
            raise

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        """
        List subscriptions to the SNS topic.
        
        Returns:
            The subscriptions
        """
        try:
            response = self.sns.list_subscriptions_by_topic(
                TopicArn=self.topic_arn,
            )
            
            subscriptions = response["Subscriptions"]
            
            self.logger.info(
                f"Listed {len(subscriptions)} subscriptions to SNS topic",
                topic_arn=self.topic_arn,
            )
            
            return subscriptions
        except ClientError as e:
            self.logger.error(
                f"Failed to list subscriptions to SNS topic: {str(e)}",
                topic_arn=self.topic_arn,
            )
            raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Publish alerts to SNS")
    parser.add_argument(
        "--topic-arn",
        type=str,
        default=settings.SNS_TOPIC_ARN,
        help="SNS topic ARN",
    )
    parser.add_argument(
        "--topic-name",
        type=str,
        default=settings.SNS_TOPIC_NAME,
        help="SNS topic name",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=settings.AWS_REGION,
        help="AWS region",
    )
    parser.add_argument(
        "--subscribe-email",
        type=str,
        help="Subscribe an email address to the SNS topic",
    )
    parser.add_argument(
        "--list-subscriptions",
        action="store_true",
        help="List subscriptions to the SNS topic",
    )
    parser.add_argument(
        "--test-alert",
        action="store_true",
        help="Send a test alert",
    )
    parser.add_argument(
        "--service",
        type=str,
        default="test-service",
        help="Service name for the test alert",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    publisher = SNSPublisher(
        topic_arn=args.topic_arn,
        topic_name=args.topic_name,
        region=args.region,
    )
    
    if args.subscribe_email:
        subscription_arn = publisher.subscribe_email(args.subscribe_email)
        print(f"Subscribed {args.subscribe_email} to SNS topic {publisher.topic_arn}")
        print(f"Subscription ARN: {subscription_arn}")
        
    if args.list_subscriptions:
        subscriptions = publisher.list_subscriptions()
        print(f"Subscriptions to SNS topic {publisher.topic_arn}:")
        for subscription in subscriptions:
            print(f"  {subscription['Protocol']}: {subscription['Endpoint']}")
            
    if args.test_alert:
        # Create a test alert
        alert_data = {
            "service": args.service,
            "anomaly_type": "error_rate",
            "error_rate": 0.15,
            "threshold": 0.1,
            "error_count": 15,
            "total_count": 100,
            "window_start": "2023-03-06T12:00:00Z",
            "window_end": "2023-03-06T12:05:00Z",
        }
        
        message_id = publisher.publish_alert(alert_data)
        print(f"Published test alert to SNS topic {publisher.topic_arn}")
        print(f"Message ID: {message_id}") 