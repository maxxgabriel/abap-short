"""
Main entry point for Sales ETL System.
Orchestrates the complete ETL pipeline execution.
"""
from pyspark.sql import SparkSession
import yaml
import argparse
from datetime import datetime, timedelta, date
import logging

from src.orchestrator import ETLOrchestrator
from src.logger import ETLLogger


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file
    
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Sales ETL Process')
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date for data extraction (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date for data extraction (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode (no commits)'
    )
    
    parser.add_argument(
        '--source-path',
        type=str,
        default=None,
        help='Source data path (optional)'
    )
    
    parser.add_argument(
        '--target-path',
        type=str,
        default=None,
        help='Target data path (optional)'
    )
    
    return parser.parse_args()


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """
    Create and configure Spark session.
    
    Args:
        app_name: Application name
        config: Configuration dictionary
    
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Apply Spark configurations from config
    spark_config = config.get('spark', {})
    for key, value in spark_config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def main():
    """Main execution function."""
    # Parse arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Parse dates
    from_date = datetime.strptime(args.from_date, '%Y-%m-%d').date()
    to_date = datetime.strptime(args.to_date, '%Y-%m-%d').date()
    
    # Validate dates
    if from_date > to_date:
        raise ValueError("From date cannot be later than to date")
    
    if to_date > date.today():
        raise ValueError("To date cannot be in the future")
    
    # Create Spark session
    spark = create_spark_session("SalesETL", config)
    
    try:
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        base_logger = logging.getLogger("SalesETL")
        
        # Display header
        print("=" * 70)
        print(" " * 20 + "Sales Data ETL Process")
        print("=" * 70)
        print(f"Processing Date Range: {from_date} to {to_date}")
        print(f"Test Mode: {'Yes' if args.test_mode else 'No'}")
        print("=" * 70)
        print()
        
        # Create orchestrator with temporary logger
        temp_logger = ETLLogger("TEMP")
        orchestrator = ETLOrchestrator(spark, config, temp_logger.logger)
        
        # Display ETL run ID
        print(f"ETL Run ID: {orchestrator.get_etl_run_id()}")
        print()
        
        # Run ETL process
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date,
            source_path=args.source_path,
            target_path=args.target_path
        )
        
        print()
        
        if success:
            print("*** ETL Process Completed Successfully ***")
            print()
            
            # Display summary
            summary = orchestrator.display_summary()
            
            if args.test_mode:
                print("Test mode - No data committed")
            else:
                print("Data committed successfully")
        else:
            print("*** ETL Process Failed ***")
            print("Please check the error logs for details.")
            
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()