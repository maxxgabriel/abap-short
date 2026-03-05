"""
Production monitoring module for Sales ETL PySpark jobs.
Tracks job execution, performance metrics, and error conditions.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession
import json
import time
import requests
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class JobMetrics:
    """Job execution metrics."""
    job_id: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    records_processed: int
    records_success: int
    records_error: int
    duration_seconds: float
    memory_used_mb: float
    cpu_utilization: float
    error_rate: float
    throughput_records_per_sec: float


@dataclass
class PerformanceMetrics:
    """Performance metrics for ETL stages."""
    stage_name: str
    duration_seconds: float
    records_processed: int
    memory_peak_mb: float
    shuffle_read_mb: float
    shuffle_write_mb: float
    task_count: int
    failed_tasks: int


@dataclass
class AlertCondition:
    """Alert condition definition."""
    name: str
    threshold: float
    operator: str  # gt, lt, eq
    metric: str
    severity: str  # critical, warning, info


class MetricsCollector:
    """Collects and aggregates metrics from Spark jobs."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        
    def collect_job_metrics(self, job_id: str) -> JobMetrics:
        """Collect metrics for a specific job."""
        # Get Spark UI data
        status_tracker = self.sc.statusTracker()
        
        # In production, would query Spark History Server API
        metrics = JobMetrics(
            job_id=job_id,
            start_time=datetime.now(),
            end_time=None,
            status='RUNNING',
            records_processed=0,
            records_success=0,
            records_error=0,
            duration_seconds=0.0,
            memory_used_mb=0.0,
            cpu_utilization=0.0,
            error_rate=0.0,
            throughput_records_per_sec=0.0
        )
        
        return metrics
        
    def collect_stage_metrics(self) -> List[PerformanceMetrics]:
        """Collect metrics for all stages."""
        status_tracker = self.sc.statusTracker()
        stage_metrics = []
        
        for stage_id in status_tracker.getJobIdsForGroup(""):
            stage_info = status_tracker.getStageInfo(stage_id)
            if stage_info:
                metrics = PerformanceMetrics(
                    stage_name=f"Stage_{stage_id}",
                    duration_seconds=0.0,
                    records_processed=0,
                    memory_peak_mb=0.0,
                    shuffle_read_mb=0.0,
                    shuffle_write_mb=0.0,
                    task_count=stage_info.numTasks,
                    failed_tasks=stage_info.numFailedTasks
                )
                stage_metrics.append(metrics)
                
        return stage_metrics
        
    def get_executor_metrics(self) -> Dict[str, Any]:
        """Get executor-level metrics."""
        # In production, would query from Spark metrics system
        return {
            'active_executors': 0,
            'total_memory_mb': 0,
            'used_memory_mb': 0,
            'total_cores': 0,
            'active_tasks': 0
        }


class HealthChecker:
    """Monitors job health and detects anomalies."""
    
    def __init__(self, alert_conditions: List[AlertCondition]):
        self.alert_conditions = alert_conditions
        self.baseline_metrics: Dict[str, float] = {}
        
    def check_health(self, metrics: JobMetrics) -> List[Dict[str, Any]]:
        """Check job health against defined conditions."""
        alerts = []
        
        for condition in self.alert_conditions:
            if self._evaluate_condition(metrics, condition):
                alert = {
                    'name': condition.name,
                    'severity': condition.severity,
                    'metric': condition.metric,
                    'threshold': condition.threshold,
                    'actual_value': getattr(metrics, condition.metric, 0),
                    'timestamp': datetime.now().isoformat()
                }
                alerts.append(alert)
                logger.warning(f"Alert triggered: {condition.name}")
                
        return alerts
        
    def _evaluate_condition(
        self,
        metrics: JobMetrics,
        condition: AlertCondition
    ) -> bool:
        """Evaluate if alert condition is met."""
        actual_value = getattr(metrics, condition.metric, 0)
        
        if condition.operator == 'gt':
            return actual_value > condition.threshold
        elif condition.operator == 'lt':
            return actual_value < condition.threshold
        elif condition.operator == 'eq':
            return actual_value == condition.threshold
            
        return False
        
    def update_baseline(self, metrics: JobMetrics):
        """Update baseline metrics for anomaly detection."""
        for field in metrics.__dataclass_fields__:
            value = getattr(metrics, field)
            if isinstance(value, (int, float)):
                if field not in self.baseline_metrics:
                    self.baseline_metrics[field] = value
                else:
                    # Exponential moving average
                    alpha = 0.3
                    self.baseline_metrics[field] = (
                        alpha * value +
                        (1 - alpha) * self.baseline_metrics[field]
                    )


class MonitoringDashboard:
    """Provides monitoring dashboard functionality."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics_history: List[JobMetrics] = []
        self.alerts_history: List[Dict[str, Any]] = []
        
    def record_metrics(self, metrics: JobMetrics):
        """Record metrics for historical tracking."""
        self.metrics_history.append(metrics)
        
        # Keep only last 1000 entries
        if len(self.metrics_history) > 1000:
            self.metrics_history = self.metrics_history[-1000:]
            
    def record_alert(self, alert: Dict[str, Any]):
        """Record alert for tracking."""
        self.alerts_history.append(alert)
        
        # Keep only last 500 alerts
        if len(self.alerts_history) > 500:
            self.alerts_history = self.alerts_history[-500:]
            
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        if not self.metrics_history:
            return {'status': 'no_data'}
            
        recent_metrics = self.metrics_history[-10:]
        
        return {
            'total_jobs': len(self.metrics_history),
            'recent_jobs': len(recent_metrics),
            'avg_duration': sum(
                m.duration_seconds for m in recent_metrics
            ) / len(recent_metrics),
            'avg_throughput': sum(
                m.throughput_records_per_sec for m in recent_metrics
            ) / len(recent_metrics),
            'total_alerts': len(self.alerts_history),
            'critical_alerts': len([
                a for a in self.alerts_history
                if a.get('severity') == 'critical'
            ]),
            'last_updated': datetime.now().isoformat()
        }
        
    def export_metrics(self, filepath: str):
        """Export metrics to file."""
        data = {
            'metrics': [asdict(m) for m in self.metrics_history],
            'alerts': self.alerts_history,
            'summary': self.get_summary()
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
            
        logger.info(f"Metrics exported to {filepath}")


class ErrorTracker:
    """Tracks and categorizes errors."""
    
    def __init__(self):
        self.errors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
    def track_error(
        self,
        error_type: str,
        error_message: str,
        context: Dict[str, Any]
    ):
        """Track an error occurrence."""
        error_record = {
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'message': error_message,
            'context': context
        }
        
        self.errors[error_type].append(error_record)
        logger.error(f"Error tracked: {error_type} - {error_message}")
        
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors."""
        return {
            'total_error_types': len(self.errors),
            'total_errors': sum(len(v) for v in self.errors.values()),
            'error_breakdown': {
                k: len(v) for k, v in self.errors.items()
            },
            'most_common_error': max(
                self.errors.items(),
                key=lambda x: len(x[1])
            )[0] if self.errors else None
        }
        
    def get_recent_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most recent errors."""
        all_errors = []
        for error_list in self.errors.values():
            all_errors.extend(error_list)
            
        all_errors.sort(
            key=lambda x: x['timestamp'],
            reverse=True
        )
        
        return all_errors[:limit]


class MonitoringService:
    """Main monitoring service orchestrator."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.collector = MetricsCollector(spark)
        self.dashboard = MonitoringDashboard(config)
        self.error_tracker = ErrorTracker()
        
        # Define alert conditions
        alert_conditions = [
            AlertCondition(
                name='high_error_rate',
                threshold=0.05,
                operator='gt',
                metric='error_rate',
                severity='critical'
            ),
            AlertCondition(
                name='low_throughput',
                threshold=100.0,
                operator='lt',
                metric='throughput_records_per_sec',
                severity='warning'
            ),
            AlertCondition(
                name='long_duration',
                threshold=3600.0,
                operator='gt',
                metric='duration_seconds',
                severity='warning'
            )
        ]
        
        self.health_checker = HealthChecker(alert_conditions)
        
    def monitor_job(self, job_id: str, interval_seconds: int = 30):
        """Monitor a running job."""
        logger.info(f"Starting monitoring for job {job_id}")
        
        while True:
            try:
                # Collect metrics
                metrics = self.collector.collect_job_metrics(job_id)
                self.dashboard.record_metrics(metrics)
                
                # Check health
                alerts = self.health_checker.check_health(metrics)
                for alert in alerts:
                    self.dashboard.record_alert(alert)
                    self._send_alert(alert)
                    
                # Update baseline
                self.health_checker.update_baseline(metrics)
                
                # Log summary
                self._log_status(metrics)
                
                # Check if job completed
                if metrics.status in ['SUCCEEDED', 'FAILED']:
                    logger.info(f"Job {job_id} completed with status {metrics.status}")
                    break
                    
                time.sleep(interval_seconds)
                
            except Exception as e:
                self.error_tracker.track_error(
                    error_type='monitoring_error',
                    error_message=str(e),
                    context={'job_id': job_id}
                )
                time.sleep(interval_seconds)
                
    def _log_status(self, metrics: JobMetrics):
        """Log current status."""
        logger.info(
            f"Job Status: {metrics.status} | "
            f"Processed: {metrics.records_processed} | "
            f"Success: {metrics.records_success} | "
            f"Errors: {metrics.records_error} | "
            f"Throughput: {metrics.throughput_records_per_sec:.2f} rec/sec"
        )
        
    def _send_alert(self, alert: Dict[str, Any]):
        """Send alert notification."""
        endpoint = self.config.get('alert_endpoint')
        if endpoint:
            try:
                requests.post(
                    endpoint,
                    json=alert,
                    timeout=5
                )
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
                
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive monitoring report."""
        return {
            'dashboard_summary': self.dashboard.get_summary(),
            'error_summary': self.error_tracker.get_error_summary(),
            'recent_errors': self.error_tracker.get_recent_errors(),
            'baseline_metrics': self.health_checker.baseline_metrics,
            'generated_at': datetime.now().isoformat()
        }


def create_monitoring_service(
    spark: SparkSession,
    config_path: str
) -> MonitoringService:
    """Factory function to create monitoring service."""
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    return MonitoringService(spark, config)