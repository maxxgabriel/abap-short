"""
Sales ETL Main Program
Migrated from ABAP Z_SALES_ETL_MAIN
Implements CLI argument parsing, datetime handling, and logging framework
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from src.orchestrator import ETLOrchestrator
from src.config import load_config
from src.utils.logger import setup_logger

def parse_arguments():
    """
    Parse command-line arguments
    Replaces ABAP SELECTION-SCREEN
    """
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py --from-date 2024-01-01 --to-date 2024-01-31
  python src/main.py --days-back 7 --test-mode
  python src/main.py --config config/custom.yaml
        """
    )
    
    # Date range parameters
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        '--from-date',
        type=str,
        help='Start date (YYYY-MM-DD format)',
        metavar='DATE'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date (YYYY-MM-DD format, default: today)',
        metavar='DATE'
    )
    
    date_group.add_argument(
        '--days-back',
        type=int,
        default=7,
        help='Number of days to look back from today (default: 7)',
        metavar='N'
    )
    
    # Processing options
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=True,
        help='Run in test mode (no data commit, default: True)'
    )
    
    parser.add_argument(
        '--production',
        action='store_true',
        help='Run in production mode (commits data)'
    )
    
    # Configuration
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)',
        metavar='PATH'
    )
    
    # Spark options
    parser.add_argument(
        '--master',
        type=str,
        default='local[*]',
        help='Spark master URL (default: local[*])',
        metavar='URL'
    )
    
    parser.add_argument(
        '--app-name',
        type=str,
        default='SalesETL',
        help='Spark application name (default: SalesETL)',
        metavar='NAME'
    )
    
    # Logging
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (optional)',
        metavar='PATH'
    )
    
    args = parser.parse_args()
    
    # Resolve test mode
    if args.production:
        args.test_mode = False
    
    # Calculate from_date if using days_back
    if not args.from_date:
        from_date = datetime.now() - timedelta(days=args.days_back)
        args.from_date = from_date.strftime('%Y-%m-%d')
    
    return args


def validate_dates(from_date_str: str, to_date_str: str) -> tuple:
    """
    Validate and parse date strings
    Replaces ABAP AT SELECTION-SCREEN validation
    
    Args:
        from_date_str: Start date string
        to_date_str: End date string
        
    Returns:
        Tuple of (from_date, to_date) as datetime objects
        
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
        raise ValueError("From Date cannot be later than To Date")
    
    if to_date > datetime.now():
        raise ValueError("To Date cannot be in the future")
    
    # Check reasonable date range (e.g., not more than 1 year)
    date_diff = (to_date - from_date).days
    if date_diff > 365:
        raise ValueError("Date range cannot exceed 365 days")
    
    return from_date, to_date


def display_header(logger):
    """Display ETL process header - replaces ABAP WRITE statements"""
    header = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║                     Sales Data ETL Process                           ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """
    logger.info(header)


def display_parameters(logger, args, from_date, to_date):
    """Display processing parameters - replaces ABAP WRITE statements"""
    logger.info("=" * 70)
    logger.info("ETL PARAMETERS")
    logger.info("=" * 70)
    logger.info(f"Processing Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    logger.info(f"Days in Range: {(to_date - from_date).days + 1}")
    logger.info(f"Test Mode: {'Yes' if args.test_mode else 'No'}")
    logger.info(f"Configuration File: {args.config}")
    logger.info(f"Spark Master: {args.master}")
    logger.info("=" * 70)


def display_summary(logger, orchestrator, success: bool, start_time: datetime):
    """Display ETL summary - replaces ABAP display_summary method"""
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("ETL PROCESS SUMMARY")
    logger.info("=" * 70)
    logger.info(f"ETL Run ID: {orchestrator.etl_run_id}")
    logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")
    
    # Display statistics if available
    stats = orchestrator.get_statistics()
    if stats:
        logger.info("-" * 70)
        logger.info("STATISTICS")
        logger.info("-" * 70)
        logger.info(f"Records Extracted: {stats.get('extracted', 0)}")
        logger.info(f"Records Transformed: {stats.get('transformed', 0)}")
        logger.info(f"Records Loaded: {stats.get('loaded', 0)}")
        logger.info(f"Errors: {stats.get('errors', 0)}")
        logger.info(f"Warnings: {stats.get('warnings', 0)}")
    
    logger.info("=" * 70)


def create_spark_session(args) -> SparkSession:
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName(args.app_name)
            .master(args.master)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def main():
    """
    Main ETL execution function
    Replaces ABAP START-OF-SELECTION and END-OF-SELECTION
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Setup logger - replaces ABAP WRITE statements
    logger = setup_logger(
        name='sales_etl_main',
        level=args.log_level,
        log_file=args.log_file
    )
    
    # Display header
    display_header(logger)
    
    try:
        # Validate dates
        from_date, to_date = validate_dates(args.from_date, args.to_date)
        
        # Display parameters
        display_parameters(logger, args, from_date, to_date)
        
        # Load configuration
        config = load_config(args.config)
        logger.info(f"Configuration loaded from: {args.config}")
        
        # Create Spark session
        logger.info("Initializing Spark session...")
        spark = create_spark_session(args)
        logger.info(f"Spark session created: {spark.version}")
        
        # Record start time
        start_time = datetime.now()
        
        # Create orchestrator instance
        logger.info("Creating ETL orchestrator...")
        orchestrator = ETLOrchestrator(
            spark=spark,
            config=config,
            test_mode=args.test_mode
        )
        
        logger.info(f"ETL Run ID: {orchestrator.etl_run_id}")
        logger.info("")
        
        # Run ETL process
        logger.info("Starting ETL process...")
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date
        )
        
        # Display summary
        logger.info("")
        display_summary(logger, orchestrator, success, start_time)
        
        # Final status
        logger.info("")
        if success:
            logger.info("*** ETL Process Completed Successfully ***")
            
            if args.test_mode:
                logger.info("Test mode - No data committed to database")
            else:
                logger.info("Data committed to database")
            
            return_code = 0
        else:
            logger.error("*** ETL Process Failed ***")
            logger.error("Please check the error logs for details.")
            return_code = 1
        
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")
        
        sys.exit(return_code)
        
    except ValueError as ve:
        logger.error(f"Validation Error: {ve}")
        sys.exit(1)
        
    except FileNotFoundError as fe:
        logger.error(f"Configuration Error: {fe}")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"*** Fatal Error ***")
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()