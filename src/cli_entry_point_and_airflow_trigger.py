===FILE: src/main.py===
"""
Main CLI Entry Point for Sales ETL Process
Replaces ABAP Z_SALES_ETL_MAIN program with Python argparse CLI
"""
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional
import logging

from src.orchestrator import ETLOrchestrator
from src.config import load_config
from src.logger import setup_logging


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments - replaces ABAP selection screen.
    
    ABAP equivalent:
    PARAMETERS: p_fdate TYPE dats DEFAULT sy-datum OBLIGATORY,
                p_tdate TYPE dats DEFAULT sy-datum OBLIGATORY.
    PARAMETERS: p_test TYPE abap_bool AS CHECKBOX DEFAULT abap_true.
    """
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process - Python Migration from ABAP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run ETL for last 7 days (default)
  python -m src.main
  
  # Run ETL for specific date range
  python -m src.main --from-date 2024-01-01 --to-date 2024-01-31
  
  # Production mode (commits data)
  python -m src.main --no-test-mode
  
  # Custom configuration
  python -m src.main --config custom_config.yaml
        """
    )
    
    # Date range parameters (ABAP: p_fdate, p_tdate)
    default_from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    default_to_date = datetime.now().strftime('%Y-%m-%d')
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=default_from_date,
        help=f'Start date for ETL processing (YYYY-MM-DD). Default: {default_from_date}'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=default_to_date,
        help=f'End date for ETL processing (YYYY-MM-DD). Default: {default_to_date}'
    )
    
    # Test mode parameter (ABAP: p_test)
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=True,
        help='Run in test mode (no commits). Default: True'
    )
    
    parser.add_argument(
        '--no-test-mode',
        dest='test_mode',
        action='store_false',
        help='Run in production mode (commits data)'
    )
    
    # Configuration file
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file. Default: config.yaml'
    )
    
    # Spark options
    parser.add_argument(
        '--spark-master',
        type=str,
        default=None,
        help='Spark master URL (overrides config)'
    )
    
    parser.add_argument(
        '--app-name',
        type=str,
        default='SalesETL',
        help='Spark application name'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level. Default: INFO'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Log file path (default: console only)'
    )
    
    return parser.parse_args()


def validate_dates(from_date_str: str, to_date_str: str) -> tuple[datetime, datetime]:
    """
    Validate date parameters - replaces ABAP AT SELECTION-SCREEN validation.
    
    ABAP equivalent:
    AT SELECTION-SCREEN.
      IF p_fdate > p_tdate.
        MESSAGE 'From Date cannot be later than To Date' TYPE 'E'.
      ENDIF.
    """
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")
    
    # Validate date range
    if from_date > to_date:
        raise ValueError("From Date cannot be later than To Date")
    
    # Validate future dates
    if to_date > datetime.now():
        raise ValueError("To Date cannot be in the future")
    
    # Warn if date range is too large
    date_diff = (to_date - from_date).days
    if date_diff > 365:
        logging.warning(f"Large date range detected: {date_diff} days")
    
    return from_date, to_date


def display_header(from_date: datetime, to_date: datetime, test_mode: bool) -> None:
    """
    Display ETL process header - replaces ABAP START-OF-SELECTION output.
    
    ABAP equivalent:
    WRITE: / |{'*' WIDTH = 70 }|,
           / |*{ 'Sales Data ETL Process' WIDTH = 68 ALIGN = CENTER }*|,
    """
    width = 70
    print("=" * width)
    print(f"{'Sales Data ETL Process':^{width}}")
    print("=" * width)
    print()
    print(f"Processing Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    print(f"Test Mode: {'Yes' if test_mode else 'No'}")
    print()


def display_summary(orchestrator: ETLOrchestrator, success: bool) -> None:
    """
    Display ETL process summary.
    
    ABAP equivalent:
    go_orchestrator->display_summary( ).
    """
    print()
    print("=" * 70)
    if success:
        print("*** ETL Process Completed Successfully ***")
    else:
        print("*** ETL Process Failed ***")
    print("=" * 70)
    
    summary = orchestrator.get_summary()
    print(f"ETL Run ID:    {summary['etl_run_id']}")
    print(f"Start Time:    {summary['start_time']}")
    print(f"End Time:      {summary['end_time']}")
    print(f"Duration:      {summary['duration_seconds']} seconds")
    print(f"Records Processed: {summary.get('records_processed', 0)}")
    print(f"Records Success:   {summary.get('records_success', 0)}")
    print(f"Records Error:     {summary.get('records_error', 0)}")
    print("=" * 70)


def main() -> int:
    """
    Main entry point - replaces ABAP START-OF-SELECTION block.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Override Spark master if provided
        if args.spark_master:
            config['spark']['master'] = args.spark_master
        
        # Validate dates
        from_date, to_date = validate_dates(args.from_date, args.to_date)
        
        # Display header
        display_header(from_date, to_date, args.test_mode)
        
        # Create orchestrator instance
        logger.info("Initializing ETL orchestrator...")
        orchestrator = ETLOrchestrator(
            config=config,
            app_name=args.app_name,
            test_mode=args.test_mode
        )
        
        # Display ETL run ID
        print(f"ETL Run ID: {orchestrator.get_etl_run_id()}")
        print()
        
        # Run ETL process
        logger.info(f"Starting ETL process for date range: {from_date} to {to_date}")
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date
        )
        
        # Display summary
        display_summary(orchestrator, success)
        
        # Handle test mode
        if args.test_mode:
            print("\nTest mode - No data committed to database")
        else:
            print("\nData committed to database")
        
        # Cleanup
        orchestrator.stop()
        
        # Return exit code
        return 0 if success else 1
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        print(f"\nError: {e}", file=sys.stderr)
        return 2
        
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        print(f"\n*** Fatal Error ***", file=sys.stderr)
        print(f"Error: {e}", file=sys.stderr)
        return 3
    
    finally:
        logging.info("ETL process terminated")


if __name__ == '__main__':
    sys.exit(main())


===FILE: src/orchestrator.py===
"""
ETL Orchestrator - Main ETL coordinator
Replaces ABAP ZCL_ETL_ORCHESTRATOR class
"""
from datetime import datetime
from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.utils import generate_etl_run_id


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    
    ABAP equivalent: ZCL_ETL_ORCHESTRATOR
    """
    
    def __init__(self, config: Dict[str, Any], app_name: str = 'SalesETL', 
                 test_mode: bool = False):
        """
        Initialize ETL orchestrator.
        
        ABAP equivalent: constructor method
        """
        self.config = config
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
        
        # Generate unique ETL run ID
        self.etl_run_id = generate_etl_run_id()
        
        # Initialize Spark session
        self.spark = self._create_spark_session(app_name)
        
        # Initialize ETL logger
        self.etl_logger = ETLLogger(
            spark=self.spark,
            etl_run_id=self.etl_run_id,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = ETLExtractor(
            spark=self.spark,
            logger=self.etl_logger,
            config=config
        )
        
        self.transformer = ETLTransformer(
            spark=self.spark,
            logger=self.etl_logger,
            config=config
        )
        
        self.loader = ETLLoader(
            spark=self.spark,
            logger=self.etl_logger,
            config=config,
            test_mode=test_mode
        )
        
        # Initialize tracking variables
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.etl_logger.log_message(
            step='INIT',
            status='S',
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create and configure Spark session."""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder.appName(app_name)
        
        # Set master
        master = spark_config.get('master', 'local[*]')
        builder = builder.master(master)
        
        # Set additional configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        log_level = spark_config.get('log_level', 'WARN')
        spark.sparkContext.setLogLevel(log_level)
        
        self.logger.info(f"Spark session created: {app_name} on {master}")
        
        return spark
    
    def run_etl(self, from_date: datetime, to_date: datetime) -> bool:
        """
        Execute the complete ETL process.
        
        ABAP equivalent: run_etl method
        
        Args:
            from_date: Start date for processing
            to_date: End date for processing
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.etl_logger.log_message(
                step='START',
                status='S',
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(from_date, to_date)
            
            if raw_df is None or raw_df.count() == 0:
                raise ValueError("Extraction returned no data")
            
            # Step 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df)
            
            if analytics_df is None or analytics_df.count() == 0:
                raise ValueError("Transformation returned no data")
            
            # Step 3: Load
            self.logger.info("=== LOAD Phase ===")
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise ValueError("Load process failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.etl_logger.log_message(
                step='COMPLETE',
                status='S',
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.exception(f"ETL process failed: {e}")
            self.etl_logger.log_message(
                step='ERROR',
                status='E',
                message=f"ETL process failed: {str(e)}"
            )
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get ETL process summary.
        
        ABAP equivalent: display_summary method
        """
        duration_seconds = 0
        if self.start_time and self.end_time:
            duration_seconds = int((self.end_time - self.start_time).total_seconds())
        
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration_seconds,
        }
        
        # Add component statistics if available
        if hasattr(self.extractor, 'get_statistics'):
            summary.update(self.extractor.get_statistics())
        
        if hasattr(self.transformer, 'get_statistics'):
            summary.update(self.transformer.get_statistics())
        
        if hasattr(self.loader, 'get_statistics'):
            summary.update(self.loader.get_statistics())
        
        return summary
    
    def stop(self) -> None:
        """Stop Spark session and cleanup resources."""
        self.logger.info("Stopping ETL orchestrator...")
        
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


===FILE: src/airflow_trigger.py===
"""
Airflow DAG for Sales ETL Process
Provides Airflow integration for scheduling and orchestration
"""
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    logging.warning("Airflow not installed - DAG definition skipped")


def create_etl_dag(
    dag_id: str = 'sales_etl_pipeline',
    schedule_interval: str = '0 2 * * *',  # Daily at 2 AM
    default_args: Dict[str, Any] = None
) -> 'DAG':
    """
    Create Airflow DAG for Sales ETL process.
    
    Args:
        dag_id: Unique identifier for the DAG
        schedule_interval: Cron expression for scheduling
        default_args: Default arguments for the DAG
        
    Returns:
        DAG: Configured Airflow DAG
    """
    if not AIRFLOW_AVAILABLE:
        raise ImportError("Airflow is not installed")
    
    # Default arguments
    if default_args is None:
        default_args = {
            'owner': 'data-engineering',
            'depends_on_past': False,
            'start_date': days_ago(1),
            'email': ['data-engineering@company.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=2),
        }
    
    # Create DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description='Sales Data ETL Pipeline - migrated from ABAP',
        schedule_interval=schedule_interval,
        catchup=False,
        tags=['etl', 'sales', 'abap-migration'],
    )
    
    # Task 1: Run ETL process
    run_etl_task = BashOperator(
        task_id='run_sales_etl',
        bash_command=(
            'python -m src.main '
            '--from-date {{ ds }} '
            '--to-date {{ ds }} '
            '--no-test-mode '
            '--config config.yaml '
            '--log-level INFO'
        ),
        dag=dag,
    )
    
    # Task 2: Validate results (optional)
    def validate_etl_results(**context):
        """Validate ETL execution results."""
        # Add validation logic here
        logging.info("Validating ETL results...")
        return True
    
    validate_task = PythonOperator(
        task_id='validate_results',
        python_callable=validate_etl_results,
        dag=dag,
    )
    
    # Task 3: Send notification (optional)
    def send_notification(**context):
        """Send success notification."""
        logging.info("ETL process completed successfully")
        # Add notification logic (email, Slack, etc.)
        return True
    
    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        dag=dag,
    )
    
    # Define task dependencies
    run_etl_task >> validate_task >> notify_task
    
    return dag


# Create DAG instance if Airflow is available
if AIRFLOW_AVAILABLE:
    # Production DAG - runs daily
    sales_etl_dag = create_etl_dag(
        dag_id='sales_etl_daily',
        schedule_interval='0 2 * * *',  # 2 AM daily
    )
    
    # Backfill DAG - runs on demand
    sales_etl_backfill_dag = create_etl_dag(
        dag_id='sales_etl_backfill',
        schedule_interval=None,  # Manual trigger only
    )


def trigger_etl_programmatically(
    from_date: str,
    to_date: str,
    test_mode: bool = False
) -> Dict[str, Any]:
    """
    Trigger ETL process programmatically (without Airflow).
    
    This function can be called from other Python code or APIs.
    
    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        test_mode: Whether to run in test mode
        
    Returns:
        dict: Execution results
    """
    from src.main import main
    import sys
    
    # Prepare arguments
    sys.argv = [
        'main.py',
        '--from-date', from_date,
        '--to-date', to_date,
    ]
    
    if test_mode:
        sys.argv.append('--test-mode')
    else:
        sys.argv.append('--no-test-mode')
    
    # Execute ETL
    exit_code = main()
    
    return {
        'success': exit_code == 0,
        'exit_code': exit_code,
        'from_date': from_date,
        'to_date': to_date,
        'test_mode': test_mode,
    }


===FILE: config.yaml===
# ETL Configuration
# Replaces ABAP configuration parameters and constants

# Spark Configuration
spark:
  master: "local[*]"  # Use "yarn" or "spark://host:port" for cluster mode
  app_name: "SalesETL"
  log_level: "WARN"
  config:
    spark.sql.shuffle.partitions: "200"
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"
    spark.sql.sources.partitionOverwriteMode: "dynamic"

# Database Configuration (replace with actual values)
database:
  source:
    type: "jdbc"  # or "hive", "delta", etc.
    url: "jdbc:postgresql://localhost:5432/sales_raw"
    driver: "org.postgresql.Driver"
    table: "zsales_raw"
    user: "${DB_USER}"  # Use environment variables
    password: "${DB_PASSWORD}"
  
  target:
    type: "jdbc"
    url: "jdbc:postgresql://localhost:5432/sales_analytics"
    driver: "org.postgresql.Driver"
    table: "zsales_analytics"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
  
  log:
    type: "jdbc"
    url: "jdbc:postgresql://localhost:5432/etl_metadata"
    driver: "org.postgresql.Driver"
    table: "zetl_log"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"

# Business Rules (from ABAP ZCL_ETL_CONSTANTS)
business_rules:
  discount:
    qty_tier1: 10
    qty_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
  
  tax:
    rate: 0.08
  
  cost:
    ratio: 0.60  # Cost is 60% of unit price
  
  category:
    high_threshold: 2000.00
    medium_threshold: 500.00

# ETL Process Configuration
etl:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600
  parallel_jobs: 4
  
  # Data quality checks
  data_quality:
    enabled: true
    min_records: 1
    max_null_percentage: 0.05
    unique_key_check: true

# Logging Configuration
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "logs/etl_process.log"
  rotation:
    max_bytes: 10485760  # 10MB
    backup_count: 5

# Status Codes (from ABAP constants)
status_codes:
  new: "N"
  processed: "P"
  error: "E"
  warning: "W"
  success: "S"
  info: "I"

# Process Steps
process_steps:
  init: "INIT"
  extract: "EXTRACT"
  transform: "TRANSFORM"
  load: "LOAD"
  validate: "VALIDATE"
  complete: "COMPLETE"
  error: "ERROR"

# Sale Categories
categories:
  high: "HIGH"
  medium: "MEDIUM"
  low: "LOW"

# ID Prefixes
id_prefixes:
  etl_run: "ETL"
  log: "LOG"
  analytics: "ANL"

# Messages
messages:
  init_success: "ETL process initialized successfully"
  extract_start: "Starting data extraction"
  extract_complete: "Data extraction completed"
  transform_start: "Starting data transformation"
  transform_complete: "Data transformation completed"
  load_start: "Starting data load"
  load_complete: "Data load completed"
  etl_complete: "ETL process completed successfully"
  etl_error: "ETL process failed"


===FILE: src/config.py===
"""
Configuration loader and manager
"""
import os
import yaml
from typing import Dict, Any
from pathlib import Path


def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment variable substitution.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        dict: Configuration dictionary
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # Substitute environment variables
    config = _substitute_env_vars(config)
    
    return config


def _substitute_env_vars(config: Any) -> Any:
    """Recursively substitute environment variables in configuration."""
    if isinstance(config, dict):
        return {k: _substitute_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_substitute_env_vars(item) for item in config]
    elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
        env_var = config[2:-1]
        return os.environ.get(env_var, config)
    else:
        return config


===FILE: tests/test_main.py===
"""
Unit tests for main CLI entry point
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys

from src.main import (
    parse_arguments,
    validate_dates,
    main
)


class TestArgumentParsing:
    """Test command-line argument parsing."""
    
    def test_default_arguments(self):
        """Test parsing with default arguments."""
        with patch.object(sys, 'argv', ['main.py']):
            args = parse_arguments()
            
            assert args.test_mode is True
            assert args.config == 'config.yaml'
            assert args.log_level == 'INFO'
    
    def test_custom_date_range(self):
        """Test parsing custom date range."""
        with patch.object(sys, 'argv', [
            'main.py',
            '--from-date', '2024-01-01',
            '--to-date', '2024-01-31'
        ]):
            args = parse_arguments()
            
            assert args.from_date == '2024-01-01'
            assert args.to_date == '2024-01-31'
    
    def test_production_mode(self):
        """Test production mode flag."""
        with patch.object(sys, 'argv', [
            'main.py',
            '--no-test-mode'
        ]):
            args = parse_arguments()
            
            assert args.test_mode is False
    
    def test_spark_master_override(self):
        """Test Spark master override."""
        with patch.object(sys, 'argv', [
            'main.py',
            '--spark-master', 'spark://localhost:7077'
        ]):
            args = parse_arguments()
            
            assert args.spark_master == 'spark://localhost:7077'


class TestDateValidation:
    """Test date validation logic."""
    
    def test_valid_date_range(self):
        """Test validation with valid date range."""
        from_date, to_date = validate_dates('2024-01-01', '2024-01-31')
        
        assert from_date == datetime(2024, 1, 1)
        assert to_date == datetime(2024, 1, 31)
    
    def test_invalid_date_format(self):
        """Test validation with invalid date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_dates('2024/01/01', '2024-01-31')
    
    def test_from_date_after_to_date(self):
        """Test validation when from_date > to_date."""
        with pytest.raises(ValueError, match="From Date cannot be later than To Date"):
            validate_dates('2024-02-01', '2024-01-01')
    
    def test_future_to_date(self):
        """Test validation with future to_date."""
        future_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        with pytest.raises(ValueError, match="To Date cannot be in the future"):
            validate_dates('2024-01-01', future_date)
    
    def test_large_date_range_warning(self, caplog):
        """Test warning for large date ranges."""
        from_date = '2023-01-01'
        to_date = '2024-12-31'
        
        validate_dates(from_date, to_date)
        
        # Should log warning but not raise exception
        assert any('Large date range' in record.message for record in caplog.records)


class TestMainExecution:
    """Test main execution flow."""
    
    @patch('src.main.ETLOrchestrator')
    @patch('src.main.load_config')
    def test_successful_execution(self, mock_load_config, mock_orchestrator_class):
        """Test successful ETL execution."""
        # Setup mocks
        mock_config = {'spark': {'master': 'local[*]'}}
        mock_load_config.return_value = mock_config
        
        mock_orchestrator = MagicMock()
        mock_orchestrator.get_etl_run_id.return_value = 'ETL20240101120000'
        mock_orchestrator.run_etl.return_value = True
        mock_orchestrator.get_summary.return_value = {
            'etl_run_id': 'ETL20240101120000',
            'start_time': '2024-01-01T12:00:00',