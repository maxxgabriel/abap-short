"""
ETL Orchestrator Module
Main orchestrator that coordinates the ETL pipeline
"""
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from typing import Tuple
import yaml

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger


class ETLOrchestrator:
    """Main ETL orchestrator coordinating the complete pipeline"""
    
    def __init__(self, config_path: str = "config.yaml", test_mode: bool = False):
        """
        Initialize the ETL orchestrator
        
        Args:
            config_path: Path to configuration file
            test_mode: Whether to run in test mode
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.test_mode = test_mode
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            log_level=self.config['logging']['level']
        ).get_logger()
        
        # Initialize ETL components
        self.extractor = ETLExtractor(self.spark, self.logger)
        self.transformer = ETLTransformer(
            self.spark, 
            self.logger, 
            self.config,
            self.etl_run_id
        )
        self.loader = ETLLoader(self.spark, self.logger, self.config)
        
        # Statistics
        self.start_time = None
        self.end_time = None
        self.stats = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': 0
        }
        
        self.logger.info(
            f"ETL process initialized with run ID: {self.etl_run_id}",
            extra={'step': 'INIT', 'status': 'S'}
        )
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config['spark']
        
        builder = SparkSession.builder \
            .appName(spark_config['app_name'])
        
        # Add configuration options
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(spark_config.get('log_level', 'WARN'))
        
        return spark
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Run the complete ETL pipeline
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Boolean indicating success or failure
        """
        try:
            # Record start time
            self.start_time = datetime.now()
            
            self.logger.info(
                f"ETL process started at {self.start_time}",
                extra={'step': 'START', 'status': 'S'}
            )
            
            # Step 1: Extract
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            if self.test_mode:
                extract_success, raw_df = self.extractor.extract_sample_data()
            else:
                extract_success, raw_df = self.extractor.extract_data(from_date, to_date)
            
            if not extract_success or raw_df is None:
                raise Exception("Extraction failed")
            
            self.stats['extracted'] = raw_df.count()
            
            # Step 2: Transform
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            transform_success, analytics_df = self.transformer.transform_data(raw_df)
            
            if not transform_success or analytics_df is None:
                raise Exception("Transformation failed")
            
            self.stats['transformed'] = analytics_df.count()
            
            # Step 3: Load
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            if self.test_mode:
                # In test mode, write to parquet
                output_path = self.config['output']['test_output_path']
                load_success, success_count, error_count = self.loader.load_to_parquet(
                    analytics_df, 
                    output_path
                )
            else:
                load_success, success_count, error_count = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise Exception("Load failed")
            
            self.stats['loaded'] = success_count
            self.stats['errors'] = error_count
            
            # Record end time
            self.end_time = datetime.now()
            
            self.logger.info(
                f"ETL process completed successfully at {self.end_time}",
                extra={'step': 'COMPLETE', 'status': 'S'}
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.error(
                f"ETL process failed: {str(e)}",
                extra={'step': 'ERROR', 'status': 'E'},
                exc_info=True
            )
            
            return False
        
        finally:
            # Clean up
            if self.spark:
                self.spark.catalog.clearCache()
    
    def display_summary(self):
        """Display ETL execution summary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:       {self.etl_run_id}")
        print(f"Start Time:       {self.start_time}")
        print(f"End Time:         {self.end_time}")
        print(f"Duration:         {duration:.2f} seconds")
        print(f"Records Extracted:    {self.stats['extracted']}")
        print(f"Records Transformed:  {self.stats['transformed']}")
        print(f"Records Loaded:       {self.stats['loaded']}")
        print(f"Records Failed:       {self.stats['errors']}")
        print("=" * 60)
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID"""
        return self.etl_run_id
    
    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()