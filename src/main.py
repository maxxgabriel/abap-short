"""
Main ETL CLI Entry Point
Executable CLI program for Sales Data ETL process
"""
import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession

from src.orchestrator import ETLOrchestrator
from src.utils.logger import ETLLogger
from src.utils.config import load_config


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Sales Data ETL Process - Extract, Transform, Load pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run ETL for last 7 days
  python -m src.main --from-date 2024-01-01 --to-date 2024-01-07
  
  # Run in test mode (no commit)
  python -m src.main --from-date 2024-01-01 --to-date 2024-01-07 --test-mode
  
  # Run with custom config
  python -m src.main --from-date 2024-01-01 --to-date 2024-01-07 --config custom_config.yaml
        """
    )
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date for extraction (YYYY-MM-DD). Default: 7 days ago'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date for extraction (YYYY-MM-DD). Default: today'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=False,
        help='Run in test mode (no database commit)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file. Default: config.yaml'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level. Default: INFO'
    )
    
    parser.add_argument(
        '--spark-master',
        type=str,
        default='local[*]',
        help='Spark master URL. Default: local[*]'
    )
    
    return parser.parse_args()


def validate_dates(from_date_str: str, to_date_str: str) -> tuple[datetime, datetime]:
    """
    Validate and parse date arguments
    
    Args:
        from_date_str: Start date string
        to_date_str: End date string
        
    Returns:
        Tuple of parsed datetime objects
        
    Raises:
        ValueError: If dates are invalid
    """
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
    
    # Validation rules
    if from_date > to_date:
        raise ValueError("From date cannot be later than to date")
    
    if to_date > datetime.now():
        raise ValueError("To date cannot be in the future")
    
    # Reasonable range check (e.g., max 1 year)
    if (to_date - from_date).days > 365:
        raise ValueError("Date range cannot exceed 365 days")
    
    return from_date, to_date


def display_header(from_date: datetime, to_date: datetime, test_mode: bool):
    """Display CLI header with execution parameters"""
    header = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║                   Sales Data ETL Process                             ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""
    print(header)
    print(f"Processing Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    print(f"Test Mode: {'Yes' if test_mode else 'No'}")
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)
    print()


def display_summary(orchestrator: ETLOrchestrator, success: bool):
    """Display execution summary"""
    print("\n" + "=" * 72)
    print("ETL Process Summary")
    print("=" * 72)
    
    summary = orchestrator.get_summary()
    
    print(f"ETL Run ID:       {summary['etl_run_id']}")
    print(f"Start Time:       {summary['start_time']}")
    print(f"End Time:         {summary['end_time']}")
    print(f"Duration:         {summary['duration_seconds']:.2f} seconds")
    print(f"Status:           {'SUCCESS' if success else 'FAILED'}")
    print()
    print(f"Records Extracted:   {summary.get('extracted', 0)}")
    print(f"Records Transformed: {summary.get('transformed', 0)}")
    print(f"Records Loaded:      {summary.get('loaded', 0)}")
    print(f"Errors:              {summary.get('errors', 0)}")
    
    print("=" * 72)


def create_spark_session(master: str, app_name: str) -> SparkSession:
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def main() -> int:
    """
    Main execution function
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate dates
        from_date, to_date = validate_dates(args.from_date, args.to_date)
        
        # Display header
        display_header(from_date, to_date, args.test_mode)
        
        # Load configuration
        config = load_config(args.config)
        
        # Create Spark session
        spark = create_spark_session(
            master=args.spark_master,
            app_name=f"SalesETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Initialize logger
        logger = ETLLogger(
            log_level=args.log_level,
            enable_console=True,
            enable_file=True
        )
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(
            spark=spark,
            config=config,
            logger=logger,
            test_mode=args.test_mode
        )
        
        print(f"ETL Run ID: {orchestrator.etl_run_id}\n")
        
        # Execute ETL process
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
        spark.stop()
        
        # Return exit code
        if success:
            print("\n✓ ETL Process Completed Successfully")
            return 0
        else:
            print("\n✗ ETL Process Failed - Check logs for details")
            return 1
            
    except ValueError as e:
        print(f"\n✗ Validation Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\n✗ Fatal Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())