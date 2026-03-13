"""
Main ETL Orchestrator for Sales Data Processing
Coordinates extraction, transformation, and loading
"""
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging
import sys
from typing import Optional

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader


class ETLOrchestrator:
    """Main orchestrator for Sales ETL process"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize ETL orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        self.config_path = config_path
        self.etl_run_id = self._generate_run_id()
        self.start_time = None
        self.end_time = None
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        # Initialize ETL components
        self.extractor = SalesExtractor(config_path)
        self.transformer = SalesTransformer(config_path)
        self.loader = SalesLoader(config_path)
        
        self.logger.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
    
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('logs/etl.log')
            ]
        )
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName(f"SalesETL_{self.etl_run_id}") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.adaptive.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def _generate_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run_etl(self, from_date: str, to_date: str, test_mode: bool = False) -> bool:
        """
        Execute complete ETL process
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            test_mode: If True, don't commit to final storage
            
        Returns:
            Success flag
        """
        self.start_time = datetime.now()
        self.logger.info("=" * 70)
        self.logger.info(f"Starting ETL Process - Run ID: {self.etl_run_id}")
        self.logger.info(f"Date Range: {from_date} to {to_date}")
        self.logger.info(f"Test Mode: {test_mode}")
        self.logger.info("=" * 70)
        
        try:
            # Phase 1: Extract
            self.logger.info("\n=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(self.spark, from_date, to_date)
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data extracted, terminating ETL")
                return False
            
            extract_count = raw_df.count()
            self.logger.info(f"Extracted {extract_count} records")
            
            # Phase 2: Transform
            self.logger.info("\n=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df, self.etl_run_id)
            
            # Validate transformed data
            valid_df, error_count = self.transformer.validate_transformed_data(analytics_df)
            transform_count = valid_df.count()
            
            self.logger.info(f"Transformed {transform_count} valid records")
            if error_count > 0:
                self.logger.warning(f"Found {error_count} invalid records")
            
            # Phase 3: Load
            self.logger.info("\n=== LOAD Phase ===")
            
            if test_mode:
                self.logger.info("Test mode: Skipping actual load, showing sample data")
                valid_df.show(10, truncate=False)
                success = True
                load_count = transform_count
                load_message = "Test mode - no data committed"
            else:
                success, load_count, load_message = self.loader.load_to_storage(valid_df)
            
            self.logger.info(load_message)
            
            # Finalize
            self.end_time = datetime.now()
            self._display_summary(extract_count, transform_count, load_count, error_count)
            
            if success:
                self.logger.info("\n*** ETL Process Completed Successfully ***")
                return True
            else:
                self.logger.error("\n*** ETL Process Failed ***")
                return False
                
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"\n*** Fatal Error in ETL Process ***", exc_info=True)
            self.logger.error(f"Error: {str(e)}")
            return False
        finally:
            self.spark.stop()
    
    def _display_summary(self, extract_count: int, transform_count: int, 
                        load_count: int, error_count: int):
        """Display ETL execution summary"""
        duration = (self.end_time - self.start_time).total_seconds()
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("ETL Process Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"ETL Run ID:           {self.etl_run_id}")
        self.logger.info(f"Start Time:           {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"End Time:             {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Duration:             {duration:.2f} seconds")
        self.logger.info("-" * 70)
        self.logger.info(f"Records Extracted:    {extract_count}")
        self.logger.info(f"Records Transformed:  {transform_count}")
        self.logger.info(f"Records Loaded:       {load_count}")
        self.logger.info(f"Errors:               {error_count}")
        self.logger.info("=" * 70)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sales ETL Process')
    parser.add_argument('--from-date', type=str, required=True, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode (no data committed)')
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Create and run orchestrator
    orchestrator = ETLOrchestrator(args.config)
    success = orchestrator.run_etl(args.from_date, args.to_date, args.test_mode)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()