#!/usr/bin/env python3
"""
Main ETL CLI Program
Migrated from ABAP Z_SALES_ETL_MAIN
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from src.orchestrator import ETLOrchestrator
from src.logger import ETLLogger
from src.config import load_config


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --from-date 2024-01-01 --to-date 2024-01-31
  %(prog)s --days-back 7
  %(prog)s --from-date 2024-01-01 --to-date 2024-01-31 --test-mode
        """
    )
    
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        '--from-date',
        type=str,
        help='Start date (YYYY-MM-DD format)'
    )
    date_group.add_argument(
        '--days-back',
        type=int,
        help='Process last N days (alternative to from-date/to-date)'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        help='End date (YYYY-MM-DD format, defaults to today)'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=False,
        help='Run in test mode (no database commits)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Override batch size from config'
    )
    
    return parser.parse_args()


def validate_dates(from_date_str, to_date_str):
    """
    Validate date parameters
    
    Args:
        from_date_str: Start date string
        to_date_str: End date string
        
    Returns:
        tuple: (from_date, to_date) as datetime objects
        
    Raises:
        ValueError: If dates are invalid
    """
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Invalid from-date format: {e}")
    
    try:
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Invalid to-date format: {e}")
    
    # Validation rules
    if from_date > to_date:
        raise ValueError("From date cannot be later than to date")
    
    today = datetime.now().date()
    if to_date > today:
        raise ValueError("To date cannot be in the future")
    
    # Warn if date range is too large
    if (to_date - from_date).days > 365:
        print(f"WARNING: Large date range ({(to_date - from_date).days} days)")
    
    return from_date, to_date


def display_header():
    """Display program header"""
    header = """
    ************************************************************************
    *                                                                      *
    *                    Sales Data ETL Process                            *
    *                                                                      *
    ************************************************************************
    """
    print(header)


def display_parameters(from_date, to_date, test_mode, config_path):
    """Display execution parameters"""
    print("\nExecution Parameters:")
    print("=" * 70)
    print(f"  Date Range:     {from_date} to {to_date}")
    print(f"  Days:           {(to_date - from_date).days + 1}")
    print(f"  Test Mode:      {'Yes' if test_mode else 'No'}")
    print(f"  Config File:    {config_path}")
    print("=" * 70)
    print()


def display_summary(orchestrator, success):
    """Display execution summary"""
    print("\n" + "=" * 70)
    print("ETL Process Summary")
    print("=" * 70)
    
    summary = orchestrator.get_summary()
    
    print(f"  ETL Run ID:     {summary['etl_run_id']}")
    print(f"  Start Time:     {summary['start_time']}")
    print(f"  End Time:       {summary['end_time']}")
    print(f"  Duration:       {summary['duration_seconds']:.2f} seconds")
    print(f"  Status:         {'SUCCESS' if success else 'FAILED'}")
    
    print("\n  Record Statistics:")
    print(f"    Extracted:    {summary['records_extracted']}")
    print(f"    Transformed:  {summary['records_transformed']}")
    print(f"    Loaded:       {summary['records_loaded']}")
    print(f"    Errors:       {summary['records_error']}")
    
    print("=" * 70)


def main():
    """Main execution function"""
    # Parse arguments
    args = parse_arguments()
    
    # Display header
    display_header()
    
    try:
        # Calculate dates
        if args.days_back:
            to_date = datetime.now().date()
            from_date = to_date - timedelta(days=args.days_back - 1)
            to_date_str = to_date.strftime('%Y-%m-%d')
            from_date_str = from_date.strftime('%Y-%m-%d')
        else:
            from_date_str = args.from_date
            to_date_str = args.to_date or datetime.now().date().strftime('%Y-%m-%d')
        
        # Validate dates
        from_date, to_date = validate_dates(from_date_str, to_date_str)
        
        # Load configuration
        config = load_config(args.config)
        
        # Override batch size if provided
        if args.batch_size:
            config['etl']['batch_size'] = args.batch_size
        
        # Display parameters
        display_parameters(from_date, to_date, args.test_mode, args.config)
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(
            config=config,
            test_mode=args.test_mode,
            log_level=args.log_level
        )
        
        print(f"ETL Run ID: {orchestrator.etl_run_id}\n")
        
        # Run ETL process
        print("=" * 70)
        print("Starting ETL Process")
        print("=" * 70)
        print()
        
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date
        )
        
        # Display summary
        print()
        display_summary(orchestrator, success)
        
        # Test mode message
        if args.test_mode:
            print("\n*** TEST MODE - No data committed to database ***")
        else:
            print("\n*** Data committed to database ***")
        
        # Exit with appropriate code
        if success:
            print("\n*** ETL Process Completed Successfully ***\n")
            sys.exit(0)
        else:
            print("\n*** ETL Process Failed ***")
            print("Please check the error logs for details.\n")
            sys.exit(1)
            
    except ValueError as e:
        print(f"\nERROR: {e}\n")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\nERROR: Configuration file not found: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()