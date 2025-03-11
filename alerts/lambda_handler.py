"""
AWS Lambda handler for processing alerts.
"""
import os
import json
import logging
import boto3
from typing import Dict, Any, List, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
sns_client = boto3.client("sns")
dynamodb_client = boto3.client("dynamodb")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for processing alerts.
    
    Args:
        event: The event data
        context: The Lambda context
        
    Returns:
        The response
    """
    logger.info("Received event: %s", json.dumps(event))
    
    try:
        # Extract the SNS message
        if "Records" in event:
            for record in event["Records"]:
                if record.get("EventSource") == "aws:sns":
                    process_sns_message(record["Sns"])
                    
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Alert processed successfully"}),
        }
    except Exception as e:
        logger.error("Error processing alert: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


def process_sns_message(sns_message: Dict[str, Any]) -> None:
    """
    Process an SNS message.
    
    Args:
        sns_message: The SNS message
    """
    logger.info("Processing SNS message: %s", json.dumps(sns_message))
    
    # Extract the message
    message = sns_message.get("Message", "{}")
    subject = sns_message.get("Subject", "")
    
    try:
        # Try to parse the message as JSON
        alert_data = extract_alert_data(message)
        
        if alert_data:
            # Store the alert in DynamoDB
            store_alert(alert_data)
            
            # Send notifications
            send_notifications(alert_data, subject)
    except Exception as e:
        logger.error("Error processing SNS message: %s", str(e))
        raise


def extract_alert_data(message: str) -> Optional[Dict[str, Any]]:
    """
    Extract alert data from a message.
    
    Args:
        message: The message
        
    Returns:
        The alert data, or None if not found
    """
    try:
        # Check if the message contains JSON
        if "Full alert data:" in message:
            # Extract the JSON part
            json_part = message.split("Full alert data:")[1].strip()
            return json.loads(json_part)
        else:
            # Try to parse the entire message as JSON
            return json.loads(message)
    except (json.JSONDecodeError, IndexError):
        logger.warning("Failed to extract alert data from message: %s", message)
        return None


def store_alert(alert_data: Dict[str, Any]) -> None:
    """
    Store an alert in DynamoDB.
    
    Args:
        alert_data: The alert data
    """
    # Get the DynamoDB table name from environment variables
    table_name = os.environ.get("ALERTS_TABLE_NAME", "loggr-alerts")
    
    try:
        # Add a timestamp if not present
        if "timestamp" not in alert_data:
            alert_data["timestamp"] = alert_data.get("window_end", "")
            
        # Convert the alert data to DynamoDB format
        item = {k: {"S" if isinstance(v, str) else "N", v} for k, v in alert_data.items()}
        
        # Store the alert
        dynamodb_client.put_item(
            TableName=table_name,
            Item=item,
        )
        
        logger.info("Stored alert in DynamoDB: %s", json.dumps(alert_data))
    except Exception as e:
        logger.error("Failed to store alert in DynamoDB: %s", str(e))
        raise


def send_notifications(alert_data: Dict[str, Any], subject: str) -> None:
    """
    Send notifications for an alert.
    
    Args:
        alert_data: The alert data
        subject: The alert subject
    """
    # Get the notification settings from environment variables
    notification_topic_arn = os.environ.get("NOTIFICATION_TOPIC_ARN", "")
    
    if not notification_topic_arn:
        logger.warning("No notification topic ARN specified")
        return
        
    try:
        # Format the notification message
        service = alert_data.get("service", "unknown")
        anomaly_type = alert_data.get("anomaly_type", "unknown")
        
        # Create a message based on the anomaly type
        if anomaly_type == "error_rate":
            message = (
                f"Error rate of {alert_data.get('error_rate', 0):.2%} exceeds threshold "
                f"of {alert_data.get('threshold', 0):.2%}.\n"
                f"Error count: {alert_data.get('error_count', 0)}\n"
                f"Total count: {alert_data.get('total_count', 0)}\n"
                f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
            )
        elif anomaly_type == "volume":
            message = (
                f"Log count of {alert_data.get('log_count', 0)} is {alert_data.get('z_score', 0):.2f} "
                f"standard deviations from the mean of {alert_data.get('mean', 0):.2f}.\n"
                f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
            )
        elif anomaly_type == "pattern":
            message = (
                f"Detected {alert_data.get('anomaly_count', 0)} anomalous logs out of "
                f"{alert_data.get('total_count', 0)} logs ({alert_data.get('anomaly_ratio', 0):.2%}).\n"
                f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
            )
        else:
            message = f"An anomaly was detected for service {service}.\n"
            message += f"Time window: {alert_data.get('window_start', '')} to {alert_data.get('window_end', '')}"
            
        # Add a link to the logs
        message += "\n\nCheck the logs for more details."
        
        # Publish the notification
        sns_client.publish(
            TopicArn=notification_topic_arn,
            Subject=subject,
            Message=message,
        )
        
        logger.info("Sent notification for alert: %s", json.dumps(alert_data))
    except Exception as e:
        logger.error("Failed to send notification: %s", str(e))
        raise 