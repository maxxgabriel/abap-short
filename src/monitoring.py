"""
Production monitoring module for ETL job performance tracking.
Provides metrics collection, error tracking, and health monitoring.
"""
import os
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict
import json

from pyspark.sql import SparkSession
from prometheus_client import Counter, Histogram, Gauge, push_to_gateway, CollectorRegistry
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class JobMetrics:
    """Data class for ETL job metrics."""
    job_id: str
    job_name: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    records_processed: int
    records_success: int
    records_error: int
    duration_seconds: Optional[float]
    error_rate: float
    throughput_records_per_sec: Optional[float]


@dataclass
class SystemMetrics:
    """Data class for system health metrics."""
    timestamp: datetime
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: float
    active_executors: int
    pending_tasks: int


class ETLMonitor:
    """Monitor ETL job execution and system health."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """Initialize ETL monitor."""
        self.spark = spark
        self.config = config
        self.job_id = self._generate_job_id()
        self.start_time = datetime.now()
        
        # Metrics storage
        self.metrics: Dict[str, Any] = defaultdict(int)
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        
        # Prometheus metrics
        self.registry = CollectorRegistry()
        self._setup_prometheus_metrics()
        
        # CloudWatch client (if using AWS)
        self.cloudwatch = None
        if config.get('monitoring', {}).get('use_cloudwatch'):
            import boto3
            self.cloudwatch = boto3.client('cloudwatch')
        
        logger.info(f"Monitoring initialized for job: {self.job_id}")
    
    def _generate_job_id(self) -> str:
        """Generate unique job ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        app_name = self.spark.sparkContext.appName
        return f"{app_name}_{timestamp}"
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics collectors."""
        # Counters
        self.records_processed = Counter(
            'etl_records_processed_total',
            'Total records processed',
            ['stage'],
            registry=self.registry
        )
        
        self.records_failed = Counter(
            'etl_records_failed_total',
            'Total records failed',
            ['stage', 'error_type'],
            registry=self.registry
        )
        
        # Histograms
        self.processing_duration = Histogram(
            'etl_processing_duration_seconds',
            'Time spent processing records',
            ['stage'],
            registry=self.registry
        )
        
        # Gauges
        self.active_executors = Gauge(
            'spark_active_executors',
            'Number of active Spark executors',
            registry=self.registry
        )
        
        self.memory_usage = Gauge(
            'spark_memory_usage_bytes',
            'Spark memory usage',
            ['type'],
            registry=self.registry
        )
    
    def track_stage_start(self, stage: str):
        """Track start of ETL stage."""
        self.metrics[f'{stage}_start_time'] = time.time()
        logger.info(f"Stage started: {stage}")
    
    def track_stage_end(self, stage: str, records_count: int):
        """Track end of ETL stage."""
        start_time = self.metrics.get(f'{stage}_start_time', time.time())
        duration = time.time() - start_time
        
        self.metrics[f'{stage}_duration'] = duration
        self.metrics[f'{stage}_records'] = records_count
        
        # Update Prometheus metrics
        self.records_processed.labels(stage=stage).inc(records_count)
        self.processing_duration.labels(stage=stage).observe(duration)
        
        throughput = records_count / duration if duration > 0 else 0
        
        logger.info(
            f"Stage completed: {stage} | "
            f"Records: {records_count} | "
            f"Duration: {duration:.2f}s | "
            f"Throughput: {throughput:.2f} records/sec"
        )
    
    def track_error(self, stage: str, error_type: str, error_message: str, 
                    record_id: Optional[str] = None):
        """Track error occurrence."""
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'stage': stage,
            'error_type': error_type,
            'error_message': error_message,
            'record_id': record_id
        }
        
        self.errors.append(error_entry)
        self.metrics['total_errors'] += 1
        
        # Update Prometheus
        self.records_failed.labels(stage=stage, error_type=error_type).inc()
        
        logger.error(f"Error in {stage}: {error_type} - {error_message}")
    
    def track_warning(self, stage: str, warning_message: str):
        """Track warning occurrence."""
        warning_entry = {
            'timestamp': datetime.now().isoformat(),
            'stage': stage,
            'message': warning_message
        }
        
        self.warnings.append(warning_entry)
        self.metrics['total_warnings'] += 1
        
        logger.warning(f"Warning in {stage}: {warning_message}")
    
    def collect_spark_metrics(self):
        """Collect Spark application metrics."""
        try:
            sc = self.spark.sparkContext
            
            # Get executor information
            status = sc.statusTracker()
            executor_info = sc._jsc.sc().getExecutorMemoryStatus()
            
            num_executors = len(executor_info)
            self.active_executors.set(num_executors)
            
            # Get stage information
            active_stages = status.getActiveStageIds()
            self.metrics['active_stages'] = len(active_stages)
            
            # Get job information
            active_jobs = status.getActiveJobIds()
            self.metrics['active_jobs'] = len(active_jobs)
            
            logger.debug(
                f"Spark metrics: Executors={num_executors}, "
                f"Active stages={len(active_stages)}, "
                f"Active jobs={len(active_jobs)}"
            )
            
        except Exception as e:
            logger.warning(f"Failed to collect Spark metrics: {e}")
    
    def get_job_metrics(self) -> JobMetrics:
        """Get comprehensive job metrics."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        total_processed = self.metrics.get('total_records_processed', 0)
        total_success = self.metrics.get('total_records_success', 0)
        total_error = self.metrics.get('total_errors', 0)
        
        error_rate = (total_error / total_processed * 100) if total_processed > 0 else 0
        throughput = total_processed / duration if duration > 0 else 0
        
        status = 'SUCCESS' if total_error == 0 else 'COMPLETED_WITH_ERRORS'
        
        return JobMetrics(
            job_id=self.job_id,
            job_name=self.spark.sparkContext.appName,
            start_time=self.start_time,
            end_time=end_time,
            status=status,
            records_processed=total_processed,
            records_success=total_success,
            records_error=total_error,
            duration_seconds=duration,
            error_rate=error_rate,
            throughput_records_per_sec=throughput
        )
    
    def publish_metrics_to_prometheus(self):
        """Publish metrics to Prometheus Pushgateway."""
        pushgateway_url = self.config.get('monitoring', {}).get('prometheus_pushgateway')
        
        if not pushgateway_url:
            logger.debug("Prometheus Pushgateway not configured")
            return
        
        try:
            push_to_gateway(
                pushgateway_url,
                job=self.spark.sparkContext.appName,
                registry=self.registry
            )
            logger.info("Metrics pushed to Prometheus")
        except Exception as e:
            logger.error(f"Failed to push metrics to Prometheus: {e}")
    
    def publish_metrics_to_cloudwatch(self):
        """Publish metrics to AWS CloudWatch."""
        if not self.cloudwatch:
            return
        
        namespace = self.config.get('monitoring', {}).get('cloudwatch_namespace', 'ETL')
        
        metrics_data = []
        
        # Prepare metrics
        for metric_name, value in self.metrics.items():
            if isinstance(value, (int, float)):
                metrics_data.append({
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(),
                    'Dimensions': [
                        {'Name': 'JobId', 'Value': self.job_id},
                        {'Name': 'Environment', 'Value': os.environ.get('ENVIRONMENT', 'production')}
                    ]
                })
        
        # Publish in batches (CloudWatch limit: 20 metrics per request)
        batch_size = 20
        for i in range(0, len(metrics_data), batch_size):
            batch = metrics_data[i:i + batch_size]
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=batch
                )
            except Exception as e:
                logger.error(f"Failed to publish metrics to CloudWatch: {e}")
    
    def send_alert(self, severity: str, message: str):
        """Send alert notification."""
        alert_config = self.config.get('monitoring', {}).get('alerts', {})
        
        if not alert_config.get('enabled'):
            return
        
        alert_data = {
            'severity': severity,
            'message': message,
            'job_id': self.job_id,
            'timestamp': datetime.now().isoformat(),
            'environment': os.environ.get('ENVIRONMENT', 'production')
        }
        
        # Send to Slack
        slack_webhook = alert_config.get('slack_webhook')
        if slack_webhook:
            self._send_slack_alert(slack_webhook, alert_data)
        
        # Send to PagerDuty
        pagerduty_key = alert_config.get('pagerduty_integration_key')
        if pagerduty_key and severity == 'CRITICAL':
            self._send_pagerduty_alert(pagerduty_key, alert_data)
        
        # Send email
        email_recipients = alert_config.get('email_recipients', [])
        if email_recipients:
            self._send_email_alert(email_recipients, alert_data)
    
    def _send_slack_alert(self, webhook_url: str, alert_data: Dict):
        """Send alert to Slack."""
        color = {
            'INFO': '#36a64f',
            'WARNING': '#ff9900',
            'ERROR': '#ff0000',
            'CRITICAL': '#990000'
        }.get(alert_data['severity'], '#808080')
        
        payload = {
            'attachments': [{
                'color': color,
                'title': f"ETL Alert - {alert_data['severity']}",
                'text': alert_data['message'],
                'fields': [
                    {'title': 'Job ID', 'value': alert_data['job_id'], 'short': True},
                    {'title': 'Environment', 'value': alert_data['environment'], 'short': True},
                    {'title': 'Timestamp', 'value': alert_data['timestamp'], 'short': False}
                ]
            }]
        }
        
        try:
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            logger.info("Alert sent to Slack")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    def _send_pagerduty_alert(self, integration_key: str, alert_data: Dict):
        """Send alert to PagerDuty."""
        payload = {
            'routing_key': integration_key,
            'event_action': 'trigger',
            'payload': {
                'summary': alert_data['message'],
                'severity': alert_data['severity'].lower(),
                'source': alert_data['job_id'],
                'timestamp': alert_data['timestamp'],
                'custom_details': alert_data
            }
        }
        
        try:
            response = requests.post(
                'https://events.pagerduty.com/v2/enqueue',
                json=payload
            )
            response.raise_for_status()
            logger.info("Alert sent to PagerDuty")
        except Exception as e:
            logger.error(f"Failed to send PagerDuty alert: {e}")
    
    def _send_email_alert(self, recipients: List[str], alert_data: Dict):
        """Send email alert using AWS SES."""
        try:
            import boto3
            ses_client = boto3.client('ses')
            
            subject = f"ETL Alert - {alert_data['severity']} - {alert_data['job_id']}"
            
            body_html = f"""
            <html>
            <body>
                <h2>ETL Job Alert</h2>
                <p><strong>Severity:</strong> {alert_data['severity']}</p>
                <p><strong>Message:</strong> {alert_data['message']}</p>
                <p><strong>Job ID:</strong> {alert_data['job_id']}</p>
                <p><strong>Environment:</strong> {alert_data['environment']}</p>
                <p><strong>Timestamp:</strong> {alert_data['timestamp']}</p>
            </body>
            </html>
            """
            
            ses_client.send_email(
                Source='etl-alerts@example.com',
                Destination={'ToAddresses': recipients},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {'Html': {'Data': body_html}}
                }
            )
            logger.info(f"Alert email sent to {len(recipients)} recipients")
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
    
    def generate_report(self) -> Dict:
        """Generate comprehensive monitoring report."""
        job_metrics = self.get_job_metrics()
        
        report = {
            'job_info': asdict(job_metrics),
            'stage_metrics': {
                stage: {
                    'duration': self.metrics.get(f'{stage}_duration', 0),
                    'records': self.metrics.get(f'{stage}_records', 0)
                }
                for stage in ['extract', 'transform', 'load']
            },
            'errors': self.errors,
            'warnings': self.warnings,
            'spark_metrics': {
                'active_executors': self.metrics.get('active_executors', 0),
                'active_stages': self.metrics.get('active_stages', 0),
                'active_jobs': self.metrics.get('active_jobs', 0)
            }
        }
        
        return report
    
    def save_report(self, output_path: str):
        """Save monitoring report to file."""
        report = self.generate_report()
        
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Monitoring report saved to: {output_path}")
    
    def finalize(self):
        """Finalize monitoring and publish final metrics."""
        # Collect final metrics
        self.collect_spark_metrics()
        
        # Publish to external systems
        self.publish_metrics_to_prometheus()
        self.publish_metrics_to_cloudwatch()
        
        # Generate and save report
        report_path = f"reports/monitoring_{self.job_id}.json"
        self.save_report(report_path)
        
        # Send completion notification
        job_metrics = self.get_job_metrics()
        if job_metrics.status == 'SUCCESS':
            self.send_alert('INFO', f"ETL job completed successfully: {self.job_id}")
        else:
            self.send_alert('ERROR', f"ETL job completed with errors: {self.job_id}")
        
        logger.info("Monitoring finalized")


class HealthChecker:
    """Health check utilities for ETL system."""
    
    def __init__(self, spark: SparkSession):
        """Initialize health checker."""
        self.spark = spark
    
    def check_spark_health(self) -> Dict[str, Any]:
        """Check Spark application health."""
        try:
            sc = self.spark.sparkContext
            
            # Check if context is active
            is_alive = not sc._jsc.sc().isStopped()
            
            # Get executor status
            executor_info = sc._jsc.sc().getExecutorMemoryStatus()
            num_executors = len(executor_info)
            
            # Get stage status
            status = sc.statusTracker()
            active_stages = len(status.getActiveStageIds())
            
            health_status = {
                'status': 'healthy' if is_alive and num_executors > 0 else 'unhealthy',
                'spark_context_alive': is_alive,
                'num_executors': num_executors,
                'active_stages': active_stages,
                'timestamp': datetime.now().isoformat()
            }
            
            return health_status
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_database_connectivity(self, jdbc_url: str, properties: Dict) -> Dict[str, Any]:
        """Check database connectivity."""
        try:
            # Test connection with simple query
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table="(SELECT 1 as test) tmp",
                properties=properties
            )
            
            df.count()
            
            return {
                'status': 'healthy',
                'connection': 'successful',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'connection': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_s3_access(self, bucket: str, prefix: str = '') -> Dict[str, Any]:
        """Check S3 bucket access."""
        try:
            import boto3
            s3_client = boto3.client('s3')
            
            # List objects to verify access
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )
            
            return {
                'status': 'healthy',
                'bucket': bucket,
                'accessible': True,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'bucket': bucket,
                'accessible': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }