#!/usr/bin/env python3
"""
Anomaly detection module for Apache Flink.
"""
import sys
import math
import datetime
import statistics
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
from sklearn.ensemble import IsolationForest

sys.path.append(".")
from config import settings
from config.utils import get_logger


class AnomalyDetector:
    """
    Detects anomalies in log data.
    """

    def __init__(
        self,
        error_rate_threshold: float = settings.ANOMALY_DETECTION["error_rate_threshold"],
        window_size_minutes: int = settings.ANOMALY_DETECTION["window_size_minutes"],
        min_log_count: int = settings.ANOMALY_DETECTION["min_log_count"],
        sensitivity: float = settings.ANOMALY_DETECTION["sensitivity"],
    ):
        """
        Initialize the anomaly detector.
        
        Args:
            error_rate_threshold: Threshold for error rate anomalies
            window_size_minutes: Size of the time window in minutes
            min_log_count: Minimum number of logs required for anomaly detection
            sensitivity: Sensitivity for anomaly detection (standard deviations)
        """
        self.logger = get_logger(service="anomaly-detector")
        self.error_rate_threshold = error_rate_threshold
        self.window_size_minutes = window_size_minutes
        self.min_log_count = min_log_count
        self.sensitivity = sensitivity
        
        # Initialize the isolation forest model
        self.isolation_forest = IsolationForest(
            n_estimators=100,
            max_samples="auto",
            contamination=0.1,
            random_state=42,
        )
        
        # Historical data for each service
        self.service_history = {}
        
        self.logger.info(
            "Initialized anomaly detector",
            error_rate_threshold=error_rate_threshold,
            window_size_minutes=window_size_minutes,
            min_log_count=min_log_count,
            sensitivity=sensitivity,
        )

    def detect_error_rate_anomaly(
        self,
        service: str,
        error_count: int,
        total_count: int,
        window_start: str,
        window_end: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Detect anomalies in error rates.
        
        Args:
            service: The service name
            error_count: The number of error logs
            total_count: The total number of logs
            window_start: The start of the time window
            window_end: The end of the time window
            
        Returns:
            A tuple of (is_anomaly, anomaly_data)
        """
        # Calculate the error rate
        error_rate = error_count / total_count if total_count > 0 else 0
        
        # Check if we have enough logs
        if total_count < self.min_log_count:
            self.logger.debug(
                f"Not enough logs for anomaly detection: {total_count} < {self.min_log_count}",
                service=service,
                error_rate=error_rate,
                error_count=error_count,
                total_count=total_count,
            )
            return False, {}
            
        # Check if the error rate exceeds the threshold
        is_anomaly = error_rate > self.error_rate_threshold
        
        anomaly_data = {
            "service": service,
            "error_rate": error_rate,
            "error_count": error_count,
            "total_count": total_count,
            "window_start": window_start,
            "window_end": window_end,
            "threshold": self.error_rate_threshold,
            "anomaly_type": "error_rate" if is_anomaly else None,
        }
        
        if is_anomaly:
            self.logger.warning(
                f"Error rate anomaly detected for service {service}",
                error_rate=error_rate,
                threshold=self.error_rate_threshold,
                error_count=error_count,
                total_count=total_count,
            )
            
        return is_anomaly, anomaly_data

    def detect_volume_anomaly(
        self,
        service: str,
        log_count: int,
        window_start: str,
        window_end: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Detect anomalies in log volume.
        
        Args:
            service: The service name
            log_count: The number of logs
            window_start: The start of the time window
            window_end: The end of the time window
            
        Returns:
            A tuple of (is_anomaly, anomaly_data)
        """
        # Initialize service history if needed
        if service not in self.service_history:
            self.service_history[service] = {
                "volume_history": [],
                "volume_mean": 0,
                "volume_stddev": 0,
            }
            
        # Get the service history
        history = self.service_history[service]
        
        # Add the current log count to the history
        history["volume_history"].append(log_count)
        
        # Keep only the last 24 hours (assuming 5-minute windows)
        max_history_size = 24 * 60 // self.window_size_minutes
        if len(history["volume_history"]) > max_history_size:
            history["volume_history"] = history["volume_history"][-max_history_size:]
            
        # Calculate the mean and standard deviation
        if len(history["volume_history"]) >= 3:  # Need at least 3 data points
            history["volume_mean"] = statistics.mean(history["volume_history"])
            history["volume_stddev"] = statistics.stdev(history["volume_history"])
            
            # Check if the current log count is an anomaly
            if history["volume_stddev"] > 0:
                z_score = abs(log_count - history["volume_mean"]) / history["volume_stddev"]
                is_anomaly = z_score > self.sensitivity
                
                anomaly_data = {
                    "service": service,
                    "log_count": log_count,
                    "window_start": window_start,
                    "window_end": window_end,
                    "mean": history["volume_mean"],
                    "stddev": history["volume_stddev"],
                    "z_score": z_score,
                    "sensitivity": self.sensitivity,
                    "anomaly_type": "volume" if is_anomaly else None,
                }
                
                if is_anomaly:
                    self.logger.warning(
                        f"Volume anomaly detected for service {service}",
                        log_count=log_count,
                        mean=history["volume_mean"],
                        stddev=history["volume_stddev"],
                        z_score=z_score,
                    )
                    
                return is_anomaly, anomaly_data
                
        return False, {}

    def detect_pattern_anomaly(
        self,
        logs: List[Dict[str, Any]],
        service: str,
        window_start: str,
        window_end: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Detect anomalies in log patterns using Isolation Forest.
        
        Args:
            logs: The logs to analyze
            service: The service name
            window_start: The start of the time window
            window_end: The end of the time window
            
        Returns:
            A tuple of (is_anomaly, anomaly_data)
        """
        # Check if we have enough logs
        if len(logs) < self.min_log_count:
            return False, {}
            
        # Extract features from logs
        features = self._extract_features(logs)
        
        # Fit the model and predict anomalies
        self.isolation_forest.fit(features)
        predictions = self.isolation_forest.predict(features)
        
        # Count anomalies
        anomaly_count = sum(1 for p in predictions if p == -1)
        anomaly_ratio = anomaly_count / len(logs) if logs else 0
        
        # Check if the anomaly ratio exceeds the threshold
        is_anomaly = anomaly_ratio > 0.1  # 10% of logs are anomalies
        
        anomaly_data = {
            "service": service,
            "anomaly_count": anomaly_count,
            "total_count": len(logs),
            "anomaly_ratio": anomaly_ratio,
            "window_start": window_start,
            "window_end": window_end,
            "anomaly_type": "pattern" if is_anomaly else None,
        }
        
        if is_anomaly:
            self.logger.warning(
                f"Pattern anomaly detected for service {service}",
                anomaly_count=anomaly_count,
                total_count=len(logs),
                anomaly_ratio=anomaly_ratio,
            )
            
        return is_anomaly, anomaly_data

    def _extract_features(self, logs: List[Dict[str, Any]]) -> np.ndarray:
        """
        Extract features from logs for anomaly detection.
        
        Args:
            logs: The logs to extract features from
            
        Returns:
            The extracted features
        """
        # Count logs by log level
        log_level_counts = {level: 0 for level in settings.LOG_LEVELS}
        for log in logs:
            log_level = log.get("log_level", "INFO")
            log_level_counts[log_level] += 1
            
        # Calculate the ratio of each log level
        total_logs = len(logs)
        log_level_ratios = {
            level: count / total_logs if total_logs > 0 else 0
            for level, count in log_level_counts.items()
        }
        
        # Extract other features
        features = []
        for log in logs:
            # Extract numeric features
            feature_vector = [
                log_level_ratios.get(log.get("log_level", "INFO"), 0),
            ]
            
            # Add additional features if available
            if "response_time_ms" in log:
                feature_vector.append(log["response_time_ms"])
            else:
                feature_vector.append(0)
                
            features.append(feature_vector)
            
        return np.array(features)

    def detect_anomalies(
        self,
        logs: List[Dict[str, Any]],
        window_start: str,
        window_end: str,
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies in logs.
        
        Args:
            logs: The logs to analyze
            window_start: The start of the time window
            window_end: The end of the time window
            
        Returns:
            A list of detected anomalies
        """
        anomalies = []
        
        # Group logs by service
        service_logs = {}
        for log in logs:
            service = log.get("service", "unknown")
            if service not in service_logs:
                service_logs[service] = []
            service_logs[service].append(log)
            
        # Detect anomalies for each service
        for service, service_logs_list in service_logs.items():
            # Count logs by log level
            log_level_counts = {level: 0 for level in settings.LOG_LEVELS}
            for log in service_logs_list:
                log_level = log.get("log_level", "INFO")
                log_level_counts[log_level] += 1
                
            # Detect error rate anomalies
            error_count = log_level_counts.get("ERROR", 0) + log_level_counts.get("CRITICAL", 0)
            total_count = len(service_logs_list)
            
            is_error_anomaly, error_anomaly_data = self.detect_error_rate_anomaly(
                service=service,
                error_count=error_count,
                total_count=total_count,
                window_start=window_start,
                window_end=window_end,
            )
            
            if is_error_anomaly:
                anomalies.append(error_anomaly_data)
                
            # Detect volume anomalies
            is_volume_anomaly, volume_anomaly_data = self.detect_volume_anomaly(
                service=service,
                log_count=total_count,
                window_start=window_start,
                window_end=window_end,
            )
            
            if is_volume_anomaly:
                anomalies.append(volume_anomaly_data)
                
            # Detect pattern anomalies
            is_pattern_anomaly, pattern_anomaly_data = self.detect_pattern_anomaly(
                logs=service_logs_list,
                service=service,
                window_start=window_start,
                window_end=window_end,
            )
            
            if is_pattern_anomaly:
                anomalies.append(pattern_anomaly_data)
                
        return anomalies 