```python
import argparse
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from src.config.etl_config import ETLConfig
from src.core.orchestrator import ETLOrchestrator

def parse_arguments():
    parser = argparse.ArgumentParser(description='Sales ETL Process')
    
    parser.add_argument(
        '--date-from',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date for extraction (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--date-to',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date for extraction (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode (no data committed)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/business_rules.yaml',
        help='Path to configuration file'
    )
    
    return parser.parse_args()

def initialize_spark():
    return SparkSession.builder \
        .appName("SalesETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def main():
    args = parse_arguments()
    
    print("=" * 70)
    print(" " * 20 + "Sales Data ETL Process")
    print("=" * 70)
    print()
    
    print(f"Processing Date Range: {args.date_from} to {args.date_to}")
    print(f"Test Mode: {'Yes' if args.test_mode else 'No'}")
    print()
    
    try:
        # Load configuration
        config = ETLConfig.load_from_yaml(args.config)
        config.test_mode = args.test_mode
        
        # Initialize Spark
        spark = initialize_spark()
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(spark, config.__dict__)
        
        print(f"ETL Run ID: {orchestrator.get_run_id()}")
        print()
        
        # Parse dates
        date_from = datetime.strptime(args.date_from, '%Y-%m-%d').date()
        date_to = datetime.strptime(args.date_to, '%Y-%m-%d').date()
        
        # Run ETL
        result = orchestrator.run_etl_pipeline(date_from, date_to, args.test_mode)
        
        print()
        print("=" * 70)
        
        if result.success:
            print("ETL Process Completed Successfully")
            print()
            
            summary = orchestrator.display_summary()
            print(f"Run ID:       {summary['run_id']}")
            print(f"Start Time:   {summary['start_time']}")
            print(f"End Time:     {summary['end_time']}")
            print(f"Duration:     {summary['duration_seconds']:.2f} seconds")
            print()
            print(f"Records Processed: {result.records_total}")
            print(f"Records Success:   {result.records_success}")
            print(f"Records Failed:    {result.records_failed}")
            
            if args.test_mode:
                print()
                print("Test mode - No data committed to database")
        else:
            print("ETL Process Failed")
            print(f"Error: {result.message}")
            sys.exit(1)
        
        print("=" * 70)
        
        spark.stop()
        
    except Exception as e:
        print(f"Fatal Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
```