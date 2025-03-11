"""
Utility functions for the Loggr application.
"""
import json
import uuid
import datetime
from typing import Dict, Any, Optional

import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)

logger = structlog.get_logger()


def get_logger(service: str = None, request_id: str = None) -> structlog.BoundLogger:
    """
    Get a logger with bound context.
    
    Args:
        service: The service name
        request_id: The request ID
        
    Returns:
        A logger with bound context
    """
    bound_logger = logger
    if service:
        bound_logger = bound_logger.bind(service=service)
    if request_id:
        bound_logger = bound_logger.bind(request_id=request_id)
    return bound_logger


def generate_request_id() -> str:
    """
    Generate a unique request ID.
    
    Returns:
        A unique request ID
    """
    return str(uuid.uuid4())


def current_timestamp() -> str:
    """
    Get the current timestamp in ISO 8601 format.
    
    Returns:
        The current timestamp in ISO 8601 format
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def parse_timestamp(timestamp_str: str) -> datetime.datetime:
    """
    Parse a timestamp string in ISO 8601 format.
    
    Args:
        timestamp_str: The timestamp string
        
    Returns:
        The parsed timestamp
    """
    return datetime.datetime.fromisoformat(timestamp_str)


def format_log_message(
    service: str,
    log_level: str,
    message: str,
    request_id: Optional[str] = None,
    additional_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Format a log message as a dictionary.
    
    Args:
        service: The service name
        log_level: The log level
        message: The log message
        request_id: The request ID (optional)
        additional_data: Additional data to include in the log (optional)
        
    Returns:
        The formatted log message as a dictionary
    """
    log_data = {
        "timestamp": current_timestamp(),
        "service": service,
        "log_level": log_level,
        "message": message,
    }
    
    if request_id:
        log_data["request_id"] = request_id
    else:
        log_data["request_id"] = generate_request_id()
        
    if additional_data:
        log_data.update(additional_data)
        
    return log_data


def serialize_log(log_data: Dict[str, Any]) -> str:
    """
    Serialize a log dictionary to a JSON string.
    
    Args:
        log_data: The log data
        
    Returns:
        The serialized log data
    """
    return json.dumps(log_data)


def deserialize_log(log_str: str) -> Dict[str, Any]:
    """
    Deserialize a JSON string to a log dictionary.
    
    Args:
        log_str: The serialized log data
        
    Returns:
        The deserialized log data
    """
    return json.loads(log_str) 