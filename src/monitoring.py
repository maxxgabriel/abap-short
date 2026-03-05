"""
Monitoring and metrics collection for ETL pipeline.
"""
from datetime import datetime
from typing import Dict, Any, List
import json


class ETLMonitor:
    """Monitors ETL execution and collects metrics."""
    
    def __init__(self, logger):
        """
        Initialize the monitor.
        
        Args:
            logger: ETL logger instance
        """
        self.logger = logger
        self.metrics = {}
        self.alerts = []
        
    def record_metric(self, metric_name: str, value: Any) -> None:
        """
        Record a metric value.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
        """
        self.metrics[metric_name] = value
        
    def check_thresholds(self, config: dict) -> List[dict]:
        """
        Check if metrics exceed defined thresholds.
        
        Args:
            config: Configuration with threshold definitions
            
        Returns:
            List of alerts
        """
        alerts = []
        thresholds = config.get("monitoring", {}).get("thresholds", {})
        
        # Check error rate
        if "load_error" in self.metrics and "load_success" in self.metrics:
            total = self.metrics["load_success"] + self.metrics["load_error"]
            if total > 0:
                error_rate = (self.metrics["load_error"] / total) * 100
                if error_rate > thresholds.get("error_rate_percent", 5):
                    alerts.append({
                        "severity": "HIGH",
                        "metric": "error_rate",
                        "value": error_rate,
                        "threshold": thresholds.get("error_rate_percent", 5),
                        "message": f"Error rate {error_rate:.2f}% exceeds threshold"
                    })
        
        # Check duration
        if "duration_seconds" in self.metrics:
            max_duration = thresholds.get("max_duration_seconds", 3600)
            if self.metrics["duration_seconds"] > max_duration:
                alerts.append({
                    "severity": "MEDIUM",
                    "metric": "duration",
                    "value": self.metrics["duration_seconds"],
                    "threshold": max_duration,
                    "message": f"Duration {self.metrics['duration_seconds']}s exceeds threshold"
                })
        
        self.alerts.extend(alerts)
        return alerts
    
    def generate_report(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Generate monitoring report.
        
        Args:
            start_time: Pipeline start time
            end_time: Pipeline end time
            
        Returns:
            Dictionary with monitoring data
        """
        duration = (end_time - start_time).total_seconds()
        self.record_metric("duration_seconds", duration)
        
        report = {
            "etl_run_id": self.logger.etl_run_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "metrics": self.metrics.copy(),
            "alerts": self.alerts.copy(),
            "logs_count": len(self.logger.get_logs())
        }
        
        # Calculate throughput
        if "extract_count" in self.metrics and duration > 0:
            report["throughput_records_per_second"] = self.metrics["extract_count"] / duration
        
        return report
    
    def export_metrics(self, output_path: str) -> None:
        """
        Export metrics to JSON file.
        
        Args:
            output_path: Path to output file
        """
        with open(output_path, 'w') as f:
            json.dump({
                "etl_run_id": self.logger.etl_run_id,
                "metrics": self.metrics,
                "alerts": self.alerts
            }, f, indent=2)