"""
Sales ETL Main Program with CLI Arguments
Migrated from ABAP Z_SALES_ETL_MAIN
"""
import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession

from src.config import ETLConfig
from src.orchestrator import ETLOrchestrator
from src.utils.logger import setup_logger


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    Replaces ABAP SELECTION-SCREEN.
    """
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default dates (last 7 days)
  python src/main.py
  
  # Run with specific date range
  python src/main.py --from-date 2024-01-01 --to-date 2024-01-31
  
  # Run in production mode
  python src/main.py --no-test-mode
  
  # Run with custom config
  python src/main.py --config custom_config.yaml
        """
    )
    
    # Date range parameters (replaces p_fdate, p_tdate)
    today = datetime.now().date()
    default_from = today - timedelta(days=7)
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=default_from.strftime('%Y-%m-%d'),
        help='From date (YYYY-MM-DD format). Default: 7 days ago'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=today.strftime('%Y-%m-%d'),
        help='To date (YYYY-MM-DD format). Default: today'
    )
    
    # Test mode parameter (replaces p_test)
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=True,
        help='Run in test mode (no data committed). Default: True'
    )
    
    parser.add_argument(
        '--no-test-mode',
        dest='test_mode',
        action='store_false',
        help='Run in production mode (data will be committed)'
    )
    
    # Configuration file
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file. Default: config.yaml'
    )
    
    # Spark master
    parser.add_argument(
        '--master',
        type=str,
        default='local[*]',
        help='Spark master URL. Default: local[*]'
    )
    
    return parser.parse_args()


def validate_dates(from_date: datetime, to_date: datetime) -> None:
    """
    Validate date parameters.
    Replaces ABAP AT SELECTION-SCREEN validation.
    
    Args:
        from_date: Start date
        to_date: End date
        
    Raises:
        ValueError: If validation fails
    """
    today = datetime.now().date()
    
    # From date cannot be later than To date
    if from_date.date() > to_date.date():
        raise ValueError("From Date cannot be later than To Date")
    
    # To date cannot be in the future
    if to_date.date() > today:
        raise ValueError("To Date cannot be in the future")


def parse_date(date_str: str) -> datetime:
    """
    Parse date string to datetime object.
    Replaces ABAP date handling.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        datetime object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def display_header() -> None:
    """
    Display program header.
    Replaces ABAP WRITE statements.
    """
    header = """
    **********************************************************************
    *                                                                    *
    *                  Sales Data ETL Process                            *
    *                                                                    *
    **********************************************************************
    """
    print(header)


def display_parameters(from_date: datetime, to_date: datetime, test_mode: bool) -> None:
    """
    Display processing parameters.
    Replaces ABAP WRITE statements for parameters.
    
    Args:
        from_date: Start date
        to_date: End date
        test_mode: Whether running in test mode
    """
    print(f"\nProcessing Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    print(f"Test Mode: {'Yes' if test_mode else 'No'}\n")


def main() -> int:
    """
    Main program execution.
    Replaces ABAP START-OF-SELECTION.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logger
    logger = setup_logger('sales_etl_main')
    
    try:
        # Display header
        display_header()
        
        # Parse dates
        from_date = parse_date(args.from_date)
        to_date = parse_date(args.to_date)
        
        # Validate dates
        validate_dates(from_date, to_date)
        
        # Display parameters
        display_parameters(from_date, to_date, args.test_mode)
        
        # Load configuration
        config = ETLConfig.from_yaml(args.config)
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Sales ETL Process") \
            .master(args.master) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        try:
            # Create orchestrator instance
            orchestrator = ETLOrchestrator(spark, config, logger)
            
            # Display ETL run ID
            print(f"ETL Run ID: {orchestrator.get_etl_run_id()}\n")
            
            # Run ETL process
            success = orchestrator.run_etl(from_date, to_date)
            
            # Display results
            print("\n" + "=" * 70 + "\n")
            
            if success:
                print("*** ETL Process Completed Successfully ***\n")
                
                # Display summary
                orchestrator.display_summary()
                
                # Handle test mode
                if args.test_mode:
                    print("\nTest mode - No data committed to database")
                    logger.info("Test mode enabled - data not persisted")
                else:
                    print("\nData committed to database")
                    logger.info("Production mode - data persisted successfully")
                
                return 0
            else:
                print("*** ETL Process Failed ***")
                print("Please check the error logs for details.")
                logger.error("ETL process failed")
                return 1
                
        finally:
            # Stop Spark session
            spark.stop()
            
    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        print(f"\n*** Validation Error ***\n{str(ve)}")
        return 1
        
    except Exception as ex:
        logger.error(f"Fatal error: {str(ex)}", exc_info=True)
        print(f"\n*** Fatal Error ***\nError: {str(ex)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())