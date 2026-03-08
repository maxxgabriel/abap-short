"""
Main ETL Entry Point with CLI and Spark Initialization
Converts ABAP report parameters to CLI arguments and implements SparkSession
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import SparkSession

from src.orchestrator import ETLOrchestrator
from src.utils.logger import ETLLogger
from src.utils.exceptions import ETLError


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments (converted from ABAP selection screen)"""
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Date range parameters (from ABAP PARAMETERS p_fdate/p_tdate)
    default_from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    default_to_date = datetime.now().strftime('%Y-%m-%d')
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=default_from_date,
        help='Start date for extraction (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=default_to_date,
        help='End date for extraction (YYYY-MM-DD format)'
    )
    
    # Test mode parameter (from ABAP p_test)
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=False,
        help='Run in test mode (no data committed)'
    )
    
    # Configuration file
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file'
    )
    
    # Spark master URL
    parser.add_argument(
        '--spark-master',
        type=str,
        default=None,
        help='Spark master URL (overrides config)'
    )
    
    # Application name
    parser.add_argument(
        '--app-name',
        type=str,
        default='SalesETL',
        help='Spark application name'
    )
    
    return parser.parse_args()


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments (from AT SELECTION-SCREEN)"""
    try:
        from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError as e:
        raise ETLError(f"Invalid date format: {e}. Use YYYY-MM-DD")
    
    # Validation: From date cannot be later than To date
    if from_date > to_date:
        raise ETLError("From Date cannot be later than To Date")
    
    # Validation: To date cannot be in the future
    if to_date > datetime.now():
        raise ETLError("To Date cannot be in the future")
    
    # Validate config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        raise ETLError(f"Configuration file not found: {args.config}")


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def initialize_spark(
    app_name: str,
    config: dict,
    master_override: Optional[str] = None
) -> SparkSession:
    """
    Initialize SparkSession with configuration
    Replaces ABAP system initialization
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Set master URL
    master = master_override or config.get('spark', {}).get('master', 'local[*]')
    builder = builder.master(master)
    
    # Set Spark configurations
    spark_config = config.get('spark', {}).get('config', {})
    for key, value in spark_config.items():
        builder = builder.config(key, value)
    
    # Enable Hive support if configured
    if config.get('spark', {}).get('enable_hive', False):
        builder = builder.enableHiveSupport()
    
    spark = builder.getOrCreate()
    
    # Set log level
    log_level = config.get('spark', {}).get('log_level', 'WARN')
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def display_header(args: argparse.Namespace) -> None:
    """Display process header (from ABAP WRITE statements)"""
    print("*" * 70)
    print("*" + " " * 68 + "*")
    print("*" + "Sales Data ETL Process".center(68) + "*")
    print("*" + " " * 68 + "*")
    print("*" * 70)
    print()
    print(f"Processing Date Range: {args.from_date} to {args.to_date}")
    print(f"Test Mode: {'Yes' if args.test_mode else 'No'}")
    print()


def display_summary(
    orchestrator: ETLOrchestrator,
    success: bool,
    test_mode: bool
) -> None:
    """Display execution summary (from ABAP display_summary method)"""
    print("\n" + "=" * 60)
    
    if success:
        print("*** ETL Process Completed Successfully ***")
        print()
        orchestrator.display_summary()
        print()
        
        if test_mode:
            print("Test mode - No data committed to database")
        else:
            print("Data committed to database")
    else:
        print("*** ETL Process Failed ***")
        print("Please check the error logs for details.")
    
    print("=" * 60)


def main() -> int:
    """
    Main entry point
    Replaces ABAP START-OF-SELECTION event
    """
    spark: Optional[SparkSession] = None
    
    try:
        # Parse and validate arguments
        args = parse_arguments()
        validate_arguments(args)
        
        # Display header
        display_header(args)
        
        # Load configuration
        config = load_config(args.config)
        
        # Initialize Spark session
        print("Initializing Spark session...")
        spark = initialize_spark(
            app_name=args.app_name,
            config=config,
            master_override=args.spark_master
        )
        print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
        print()
        
        # Create orchestrator instance
        orchestrator = ETLOrchestrator(
            spark=spark,
            config=config,
            test_mode=args.test_mode
        )
        
        # Display ETL run ID
        print(f"ETL Run ID: {orchestrator.get_etl_run_id()}")
        print()
        
        # Run ETL process
        success = orchestrator.run_etl(
            from_date=args.from_date,
            to_date=args.to_date
        )
        
        # Display summary
        display_summary(orchestrator, success, args.test_mode)
        
        return 0 if success else 1
        
    except ETLError as e:
        print(f"\n*** ETL Error ***", file=sys.stderr)
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
        
    except Exception as e:
        print(f"\n*** Fatal Error ***", file=sys.stderr)
        print(f"Error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Stop Spark session
        if spark:
            print("\nStopping Spark session...")
            spark.stop()


if __name__ == '__main__':
    sys.exit(main())