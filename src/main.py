"""
Main ETL Application Entry Point
"""
import argparse
from datetime import datetime, timedelta
from src.orchestrator import ETLOrchestrator


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Sales ETL Process')
    
    parser.add_argument(
        '--from-date',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date in YYYY-MM-DD format (default: 7 days ago)'
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date in YYYY-MM-DD format (default: today)'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode with sample data'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for ETL application"""
    # Parse arguments
    args = parse_arguments()
    
    # Display header
    print("*" * 70)
    print("*" + " " * 68 + "*")
    print("*" + "Sales Data ETL Process".center(68) + "*")
    print("*" + " " * 68 + "*")
    print("*" * 70)
    print()
    
    # Display parameters
    print(f"Processing Date Range: {args.from_date} to {args.to_date}")
    print(f"Test Mode: {'Yes' if args.test_mode else 'No'}")
    print(f"Config File: {args.config}")
    print()
    
    try:
        # Create orchestrator instance
        orchestrator = ETLOrchestrator(
            config_path=args.config,
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
        
        # Display results
        print()
        
        if success:
            print("*** ETL Process Completed Successfully ***")
            print()
            orchestrator.display_summary()
            
            if args.test_mode:
                print("\nTest mode - No data committed to database")
            else:
                print("\nData committed to database")
        else:
            print("*** ETL Process Failed ***")
            print("Please check the error logs for details.")
        
        # Clean up
        orchestrator.stop()
        
        # Exit with appropriate code
        exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n*** Fatal Error ***")
        print(f"Error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()