#!/usr/bin/env python3
"""
Kafka producer for simulating microservices sending logs.
"""
import sys
import time
import random
import argparse
from typing import Dict, Any, List, Optional

from kafka import KafkaProducer

sys.path.append(".")
from config import settings
from config.utils import (
    format_log_message,
    serialize_log,
    get_logger,
    generate_request_id,
)


class LogProducer:
    """
    Simulates microservices sending logs to Kafka.
    """

    def __init__(self, bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS):
        """
        Initialize the log producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.logger = get_logger(service="log-producer")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: serialize_log(x).encode("utf-8"),
        )
        self.topic = settings.KAFKA_TOPICS["raw_logs"]
        self.logger.info("Initialized Kafka producer", bootstrap_servers=bootstrap_servers)

    def generate_log(
        self,
        service: str,
        log_level: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a random log message.
        
        Args:
            service: The service name
            log_level: The log level (optional, random if not provided)
            request_id: The request ID (optional, generated if not provided)
            
        Returns:
            The generated log message
        """
        if not log_level:
            log_level = random.choice(settings.LOG_LEVELS)
            
        if not request_id:
            request_id = generate_request_id()
            
        # Generate different types of messages based on log level
        if log_level == "DEBUG":
            messages = [
                f"Processing request {request_id}",
                f"Database query executed in 15ms",
                f"Cache hit for user profile",
                f"API request parameters: {{'user_id': 123, 'action': 'view'}}",
            ]
        elif log_level == "INFO":
            messages = [
                f"User logged in successfully",
                f"Payment processed for order #12345",
                f"Email notification sent to user",
                f"New user registered: user123",
            ]
        elif log_level == "WARNING":
            messages = [
                f"High CPU usage detected: 85%",
                f"Database connection pool reaching limit (80%)",
                f"API rate limit approaching for client",
                f"Slow query detected (execution time: 2.5s)",
            ]
        elif log_level == "ERROR":
            messages = [
                f"Failed to connect to database after 3 retries",
                f"Payment gateway timeout after 30s",
                f"User authentication failed: invalid credentials",
                f"API request failed with status code 500",
            ]
        else:  # CRITICAL
            messages = [
                f"Database connection lost",
                f"Service is unresponsive",
                f"Out of memory error",
                f"Unhandled exception in main thread",
            ]
            
        message = random.choice(messages)
        
        # Add additional data based on the service and log level
        additional_data = {}
        
        if service == "auth-service":
            additional_data["user_id"] = f"user_{random.randint(1000, 9999)}"
            if log_level in ["ERROR", "CRITICAL"]:
                additional_data["auth_method"] = random.choice(["password", "oauth", "2fa"])
                
        elif service == "payment-service":
            additional_data["order_id"] = f"order_{random.randint(10000, 99999)}"
            additional_data["amount"] = round(random.uniform(10.0, 1000.0), 2)
            if log_level in ["ERROR", "CRITICAL"]:
                additional_data["payment_provider"] = random.choice(["stripe", "paypal", "braintree"])
                
        elif service == "api-gateway":
            additional_data["endpoint"] = random.choice(["/api/users", "/api/orders", "/api/products"])
            additional_data["method"] = random.choice(["GET", "POST", "PUT", "DELETE"])
            additional_data["response_time_ms"] = random.randint(10, 500)
            
        return format_log_message(
            service=service,
            log_level=log_level,
            message=message,
            request_id=request_id,
            additional_data=additional_data,
        )

    def send_log(self, log_data: Dict[str, Any]) -> None:
        """
        Send a log message to Kafka.
        
        Args:
            log_data: The log data to send
        """
        self.producer.send(self.topic, log_data)
        self.logger.debug(
            "Sent log message",
            service=log_data["service"],
            log_level=log_data["log_level"],
            request_id=log_data["request_id"],
        )

    def simulate_service_logs(
        self,
        service: str,
        rate: int = 10,
        error_probability: float = 0.1,
        duration: Optional[int] = None,
    ) -> None:
        """
        Simulate logs from a service at a given rate.
        
        Args:
            service: The service name
            rate: The number of logs per second
            error_probability: The probability of generating an error log
            duration: The duration in seconds (optional, runs indefinitely if not provided)
        """
        self.logger.info(
            "Starting log simulation",
            service=service,
            rate=rate,
            error_probability=error_probability,
            duration=duration,
        )
        
        start_time = time.time()
        count = 0
        
        try:
            while True:
                # Determine if this should be an error log
                if random.random() < error_probability:
                    log_level = random.choice(["ERROR", "CRITICAL"])
                else:
                    log_level = random.choice(["DEBUG", "INFO", "WARNING"])
                
                # Generate and send the log
                log_data = self.generate_log(service=service, log_level=log_level)
                self.send_log(log_data)
                
                count += 1
                
                # Check if we should stop
                if duration and time.time() - start_time >= duration:
                    break
                    
                # Sleep to maintain the desired rate
                time.sleep(1 / rate)
                
                # Print stats every 100 logs
                if count % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = count / elapsed if elapsed > 0 else 0
                    self.logger.info(
                        "Log simulation stats",
                        count=count,
                        elapsed_seconds=round(elapsed, 2),
                        actual_rate=round(actual_rate, 2),
                    )
                    
        except KeyboardInterrupt:
            self.logger.info("Log simulation stopped by user")
        finally:
            elapsed = time.time() - start_time
            actual_rate = count / elapsed if elapsed > 0 else 0
            self.logger.info(
                "Log simulation completed",
                count=count,
                elapsed_seconds=round(elapsed, 2),
                actual_rate=round(actual_rate, 2),
            )
            self.producer.flush()
            self.producer.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Simulate logs from a microservice")
    parser.add_argument(
        "--service",
        type=str,
        choices=settings.SERVICES,
        default="auth-service",
        help="The service to simulate logs from",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="The number of logs per second",
    )
    parser.add_argument(
        "--error-probability",
        type=float,
        default=0.1,
        help="The probability of generating an error log",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="The duration in seconds (runs indefinitely if not provided)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=settings.KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    producer = LogProducer(bootstrap_servers=args.bootstrap_servers)
    producer.simulate_service_logs(
        service=args.service,
        rate=args.rate,
        error_probability=args.error_probability,
        duration=args.duration,
    ) 