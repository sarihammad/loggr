"""
Configuration settings for the Loggr application.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "loggr")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "loggr-consumer-group")
KAFKA_TOPICS = {
    "raw_logs": f"{KAFKA_TOPIC_PREFIX}-raw-logs",
    "processed_logs": f"{KAFKA_TOPIC_PREFIX}-processed-logs",
    "alerts": f"{KAFKA_TOPIC_PREFIX}-alerts",
}

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "loggr-logs")
S3_PREFIX = os.getenv("S3_PREFIX", "logs")
S3_RETENTION_DAYS = int(os.getenv("S3_RETENTION_DAYS", "90"))

# DynamoDB Configuration
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "loggr-logs")
DYNAMODB_THROUGHPUT = {
    "ReadCapacityUnits": int(os.getenv("DYNAMODB_READ_CAPACITY_UNITS", "5")),
    "WriteCapacityUnits": int(os.getenv("DYNAMODB_WRITE_CAPACITY_UNITS", "5")),
}

# SNS Configuration
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "loggr-alerts")

# Flink Configuration
FLINK_PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
FLINK_CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "60000"))  # ms
FLINK_STATE_BACKEND = os.getenv("FLINK_STATE_BACKEND", "filesystem")
FLINK_CHECKPOINT_DIR = os.getenv("FLINK_CHECKPOINT_DIR", "/tmp/flink-checkpoints")

# Anomaly Detection Configuration
ANOMALY_DETECTION = {
    "error_rate_threshold": float(os.getenv("ANOMALY_ERROR_RATE_THRESHOLD", "0.1")),
    "window_size_minutes": int(os.getenv("ANOMALY_WINDOW_SIZE_MINUTES", "5")),
    "min_log_count": int(os.getenv("ANOMALY_MIN_LOG_COUNT", "100")),
    "sensitivity": float(os.getenv("ANOMALY_SENSITIVITY", "3.0")),  # Standard deviations
}

# Log Levels
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Services
SERVICES = [
    "auth-service",
    "user-service",
    "payment-service",
    "notification-service",
    "api-gateway",
] 