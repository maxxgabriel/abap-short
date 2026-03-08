"""
ETL monitoring and metrics collection module.
Tracks job performance, errors, and system health.
"""

import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max


@dataclass
class ETLMetrics:
    """ETL job execution metrics"""
    job_id: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    status: str
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_message: Optional[str]
    memory_usage_mb: Optional[float]
    cpu_usage_percent: Optional[float]


@dataclass
class HealthCheck:
    """System health check result"""
    timestamp: datetime
    component: str
    status: str  # healthy, degraded, unhealthy
    latency_ms: Optional[float]
    error_message: Optional[str]
    details: Dict[str, Any]


class ETLMonitor:
    """Monitors ETL job execution and system health"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ETL monitor
        
        Args:
            config: Monitoring configuration dictionary
        """
        self.config = config
        self.logger = self._setup_logging()
        self.metrics_buffer: List[ETLMetrics] = []
        self.health_checks: List[HealthCheck] = []
        self.alert_thresholds = config.get('alerts', {})
        
    def _setup_logging(self) -> logging.Logger:
        """Configure monitoring logger"""
        log_dir = Path('logs/monitoring')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('etl_monitor')
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = log_dir / f"monitor_{datetime.now().strftime('%Y%m%d')}.log"
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def start_job_monitoring(self, job_id: str) -> ETLMetrics:
        """
        Start monitoring a new ETL job
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            ETLMetrics object for tracking
        """
        metrics = ETLMetrics(
            job_id=job_id,
            start_time=datetime.now(),
            end_time=None,
            duration_seconds=None,
            status='RUNNING',
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_message=None,
            memory_usage_mb=None,
            cpu_usage_percent=None
        )
        
        self.metrics_buffer.append(metrics)
        self.logger.info(f"Started monitoring job: {job_id}")
        
        return metrics
    
    def update_job_metrics(
        self,
        job_id: str,
        extracted: int = 0,
        transformed: int = 0,
        loaded: int = 0,
        failed: int = 0
    ):
        """Update job metrics during execution"""
        for metrics in self.metrics_buffer:
            if metrics.job_id == job_id:
                metrics.records_extracted += extracted
                metrics.records_transformed += transformed
                metrics.records_loaded += loaded
                metrics.records_failed += failed
                break
    
    def complete_job_monitoring(
        self,
        job_id: str,
        status: str = 'SUCCESS',
        error_message: Optional[str] = None
    ):
        """
        Complete job monitoring and finalize metrics
        
        Args:
            job_id: Job identifier
            status: Final job status (SUCCESS, FAILED, CANCELLED)
            error_message: Error description if failed
        """
        for metrics in self.metrics_buffer:
            if metrics.job_id == job_id:
                metrics.end_time = datetime.now()
                metrics.duration_seconds = (
                    metrics.end_time - metrics.start_time
                ).total_seconds()
                metrics.status = status
                metrics.error_message = error_message
                
                self.logger.info(
                    f"Job {job_id} completed with status {status} "
                    f"in {metrics.duration_seconds:.2f}s"
                )
                
                # Check alert thresholds
                self._check_alert_thresholds(metrics)
                
                # Persist metrics
                self._persist_metrics(metrics)
                break
    
    def _check_alert_thresholds(self, metrics: ETLMetrics):
        """Check if metrics exceed alert thresholds"""
        # Duration threshold
        max_duration = self.alert_thresholds.get('max_duration_seconds', 3600)
        if metrics.duration_seconds and metrics.duration_seconds > max_duration:
            self.logger.warning(
                f"Job {metrics.job_id} exceeded duration threshold: "
                f"{metrics.duration_seconds:.2f}s > {max_duration}s"
            )
            self._send_alert('DURATION_EXCEEDED', metrics)
        
        # Error rate threshold
        total_records = (
            metrics.records_extracted + metrics.records_failed
        )
        if total_records > 0:
            error_rate = metrics.records_failed / total_records
            max_error_rate = self.alert_thresholds.get('max_error_rate', 0.05)
            
            if error_rate > max_error_rate:
                self.logger.warning(
                    f"Job {metrics.job_id} exceeded error rate threshold: "
                    f"{error_rate:.2%} > {max_error_rate:.2%}"
                )
                self._send_alert('ERROR_RATE_EXCEEDED', metrics)
    
    def _send_alert(self, alert_type: str, metrics: ETLMetrics):
        """Send alert notification"""
        alert_config = self.config.get('alert_channels', {})
        
        alert_data = {
            'type': alert_type,
            'job_id': metrics.job_id,
            'timestamp': datetime.now().isoformat(),
            'metrics': asdict(metrics)
        }
        
        # Log alert
        self.logger.error(f"ALERT: {alert_type} for job {metrics.job_id}")
        
        # Write to alert file
        if alert_config.get('file', {}).get('enabled', False):
            alert_dir = Path('logs/alerts')
            alert_dir.mkdir(parents=True, exist_ok=True)
            
            alert_file = alert_dir / f"alert_{datetime.now().strftime('%Y%m%d')}.json"
            with open(alert_file, 'a') as f:
                json.dump(alert_data, f, default=str)
                f.write('\n')
    
    def _persist_metrics(self, metrics: ETLMetrics):
        """Persist metrics to storage"""
        metrics_dir = Path('logs/metrics')
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        # Write to daily metrics file
        date_str = metrics.start_time.strftime('%Y%m%d')
        metrics_file = metrics_dir / f"metrics_{date_str}.jsonl"
        
        with open(metrics_file, 'a') as f:
            json.dump(asdict(metrics), f, default=str)
            f.write('\n')
    
    def perform_health_check(self, spark: SparkSession) -> HealthCheck:
        """
        Perform comprehensive system health check
        
        Args:
            spark: Active SparkSession
            
        Returns:
            HealthCheck result
        """
        start_time = time.time()
        details = {}
        status = 'healthy'
        error_message = None
        
        try:
            # Check Spark session
            details['spark_version'] = spark.version
            details['spark_active'] = spark.sparkContext._jsc.sc().isStopped() == False
            
            # Check catalog access
            try:
                databases = spark.catalog.listDatabases()
                details['catalog_accessible'] = True
                details['database_count'] = len(databases)
            except Exception as e:
                details['catalog_accessible'] = False
                status = 'degraded'
                self.logger.warning(f"Catalog access issue: {str(e)}")
            
            # Check executor status
            try:
                executor_info = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
                active_executors = len(executor_info)
                details['active_executors'] = active_executors
                
                min_executors = self.config.get('health_check', {}).get('min_executors', 1)
                if active_executors < min_executors:
                    status = 'degraded'
                    self.logger.warning(
                        f"Low executor count: {active_executors} < {min_executors}"
                    )
            except Exception as e:
                details['executor_check_failed'] = str(e)
                status = 'degraded'
            
            # Calculate latency
            latency_ms = (time.time() - start_time) * 1000
            
            # Check latency threshold
            max_latency = self.config.get('health_check', {}).get('max_latency_ms', 1000)
            if latency_ms > max_latency:
                status = 'degraded'
                self.logger.warning(f"High latency: {latency_ms:.2f}ms")
            
        except Exception as e:
            status = 'unhealthy'
            error_message = str(e)
            self.logger.error(f"Health check failed: {str(e)}")
        
        health_check = HealthCheck(
            timestamp=datetime.now(),
            component='etl_system',
            status=status,
            latency_ms=latency_ms,
            error_message=error_message,
            details=details
        )
        
        self.health_checks.append(health_check)
        self.logger.info(f"Health check completed: {status} ({latency_ms:.2f}ms)")
        
        return health_check
    
    def get_job_statistics(
        self,
        spark: SparkSession,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Calculate job statistics for date range
        
        Args:
            spark: Active SparkSession
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary with aggregated statistics
        """
        self.logger.info(f"Calculating statistics from {start_date} to {end_date}")
        
        try:
            # Load metrics from files
            metrics_data = []
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            current_dt = start_dt
            while current_dt <= end_dt:
                date_str = current_dt.strftime('%Y%m%d')
                metrics_file = Path('logs/metrics') / f"metrics_{date_str}.jsonl"
                
                if metrics_file.exists():
                    with open(metrics_file, 'r') as f:
                        for line in f:
                            metrics_data.append(json.loads(line))
                
                current_dt += timedelta(days=1)
            
            if not metrics_data:
                return {'error': 'No metrics data found for date range'}
            
            # Create DataFrame
            df = spark.createDataFrame(metrics_data)
            
            # Calculate statistics
            stats = df.agg(
                count('*').alias('total_jobs'),
                spark_sum(col('records_extracted')).alias('total_extracted'),
                spark_sum(col('records_loaded')).alias('total_loaded'),
                spark_sum(col('records_failed')).alias('total_failed'),
                avg('duration_seconds').alias('avg_duration_seconds'),
                spark_max('duration_seconds').alias('max_duration_seconds')
            ).collect()[0]
            
            # Status distribution
            status_dist = df.groupBy('status').count().collect()
            
            result = {
                'date_range': {
                    'start': start_date,
                    'end': end_date
                },
                'total_jobs': stats['total_jobs'],
                'total_records': {
                    'extracted': stats['total_extracted'],
                    'loaded': stats['total_loaded'],
                    'failed': stats['total_failed']
                },
                'duration': {
                    'average_seconds': float(stats['avg_duration_seconds'] or 0),
                    'max_seconds': float(stats['max_duration_seconds'] or 0)
                },
                'status_distribution': {
                    row['status']: row['count'] for row in status_dist
                },
                'success_rate': (
                    stats_dist.get('SUCCESS', 0) / stats['total_jobs']
                    if stats['total_jobs'] > 0 else 0
                )
            }
            
            self.logger.info(f"Statistics calculated: {result['total_jobs']} jobs processed")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to calculate statistics: {str(e)}")
            return {'error': str(e)}
    
    def generate_dashboard_data(self, spark: SparkSession) -> Dict[str, Any]:
        """
        Generate data for monitoring dashboard
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Dashboard data dictionary
        """
        # Get recent health checks
        recent_health = self.health_checks[-10:] if self.health_checks else []
        
        # Get recent metrics
        recent_metrics = self.metrics_buffer[-20:] if self.metrics_buffer else []
        
        # Calculate today's statistics
        today = datetime.now().strftime('%Y-%m-%d')
        today_stats = self.get_job_statistics(spark, today, today)
        
        dashboard = {
            'timestamp': datetime.now().isoformat(),
            'system_health': {
                'current_status': recent_health[-1].status if recent_health else 'unknown',
                'recent_checks': [
                    {
                        'timestamp': hc.timestamp.isoformat(),
                        'status': hc.status,
                        'latency_ms': hc.latency_ms
                    }
                    for hc in recent_health
                ]
            },
            'today_summary': today_stats,
            'recent_jobs': [
                {
                    'job_id': m.job_id,
                    'status': m.status,
                    'duration_seconds': m.duration_seconds,
                    'records_processed': m.records_loaded
                }
                for m in recent_metrics
            ]
        }
        
        return dashboard


def main():
    """Main monitoring entry point for testing"""
    import yaml
    
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize monitor
    monitor = ETLMonitor(config.get('monitoring', {}))
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ETL_Monitor_Test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Perform health check
        health = monitor.perform_health_check(spark)
        print(f"Health Status: {health.status}")
        print(f"Details: {json.dumps(health.details, indent=2)}")
        
        # Generate dashboard
        dashboard = monitor.generate_dashboard_data(spark)
        print(f"\nDashboard Data:")
        print(json.dumps(dashboard, indent=2, default=str))
        
    finally:
        spark.stop()


if __name__ == '__main__':
    main()