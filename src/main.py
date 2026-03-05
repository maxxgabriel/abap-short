"""
Main ETL CLI program for Sales Data Processing
Migrated from ABAP Z_SALES_ETL_MAIN
"""

import argparse
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional

from src.orchestrator import ETLOrchestrator
from src.config import Config
from src.logger import setup_logging


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py --from-date 2024-01-01 --to-date 2024-01-31
  python src/main.py --from-date 2024-01-01 --to-date 2024-01-31 --test-mode
  python src/main.py --config config/custom_config.yaml
        """
    )
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date for ETL processing (YYYY-MM-DD). Default: 7 days ago'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date for ETL processing (YYYY-MM-DD). Default: today'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=False,
        help='Run in test mode (no data committed to database)'
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
        '--log-file',
        type=str,
        default=None,
        help='Log file path. Default: logs/etl_YYYYMMDD_HHMMSS.log'
    )
    
    return parser.parse_args()


def validate_dates(from_date_str: str, to_date_str: str) -> tuple[datetime, datetime]:
    """
    Validate and parse date strings.
    
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
    
    if from_date > to_date:
        raise ValueError("From Date cannot be later than To Date")
    
    if to_date > datetime.now():
        raise ValueError("To Date cannot be in the future")
    
    return from_date, to_date


def print_header():
    """Print ETL process header."""
    print("*" * 70)
    print("*" + " " * 68 + "*")
    print(f"*{'Sales Data ETL Process':^68}*")
    print("*" + " " * 68 + "*")
    print("*" * 70)
    print()


def print_parameters(from_date: datetime, to_date: datetime, test_mode: bool):
    """Print processing parameters."""
    print(f"Processing Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    print(f"Test Mode: {'Yes' if test_mode else 'No'}")
    print()


def print_summary(orchestrator: ETLOrchestrator, success: bool, test_mode: bool):
    """Print ETL process summary."""
    print()
    print()
    
    if success:
        print("*** ETL Process Completed Successfully ***")
        print()
        orchestrator.display_summary()
        
        if test_mode:
            print()
            print("Test mode - No data committed to database")
        else:
            print()
            print("Data committed to database")
    else:
        print("*** ETL Process Failed ***")
        print("Please check the error logs for details.")


def main() -> int:
    """
    Main entry point for ETL process.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging(
        log_level=args.log_level,
        log_file=args.log_file
    )
    
    try:
        # Print header
        print_header()
        
        # Validate dates
        from_date, to_date = validate_dates(args.from_date, args.to_date)
        
        # Print parameters
        print_parameters(from_date, to_date, args.test_mode)
        
        # Load configuration
        config = Config.from_yaml(args.config)
        
        # Create orchestrator instance
        logger.info("Initializing ETL orchestrator")
        orchestrator = ETLOrchestrator(config=config, test_mode=args.test_mode)
        
        # Display ETL run ID
        print(f"ETL Run ID: {orchestrator.get_etl_run_id()}")
        print()
        
        # Run ETL process
        logger.info(f"Starting ETL process for date range {from_date} to {to_date}")
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date
        )
        
        # Print summary
        print_summary(orchestrator, success, args.test_mode)
        
        # Return appropriate exit code
        return 0 if success else 1
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        print(f"\n*** Error: {e} ***", file=sys.stderr)
        return 1
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        print(f"\n*** Error: Configuration file not found: {e} ***", file=sys.stderr)
        return 1
        
    except Exception as e:
        logger.exception("Fatal error occurred")
        print(f"\n*** Fatal Error ***", file=sys.stderr)
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())