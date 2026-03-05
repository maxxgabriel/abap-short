"""
ETL Orchestrator - coordinates the ETL process
Migrated from ABAP ZCL_ETL_ORCHESTRATOR
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from src.config import Config
from src.etl_logger import ETLLogger
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.utils import generate_etl_run_id


class ETLOrchestrator:
    """Main ETL orchestrator that coordinates the ETL process."""
    
    def __init__(self, config: Config, test_mode: bool = False):
        """
        Initialize the orchestrator.
        
        Args:
            config: ETL configuration
            test_mode: Whether to run in test mode
        """
        self.config = config
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
        
        # Generate unique ETL run ID
        self.etl_run_id = generate_etl_run_id()
        
        # Initialize Spark session
        self.spark = self._initialize_spark()
        
        # Initialize ETL logger
        self.etl_logger = ETLLogger(
            spark=self.spark,
            etl_run_id=self.etl_run_id,
            test_mode=test_mode
        )
        
        # Initialize ETL components
        self.extractor = Extractor(
            spark=self.spark,
            config=config,
            etl_logger=self.etl_logger
        )
        
        self.transformer = Transformer(
            spark=self.spark,
            config=config,
            etl_logger=self.etl_logger
        )
        
        self.loader = Loader(
            spark=self.spark,
            config=config,
            etl_logger=self.etl_logger,
            test_mode=test_mode
        )
        
        # Execution timing
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.etl_logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
        
        self.logger.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
    
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with configuration."""
        builder = SparkSession.builder \
            .appName(f"SalesETL_{self.etl_run_id}") \
            .config("spark.sql.session.timeZone", "UTC")
        
        # Add configuration from config file
        for key, value in self.config.spark_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def run_etl(self, from_date: datetime, to_date: datetime) -> bool:
        """
        Run the complete ETL process.
        
        Args:
            from_date: Start date for processing
            to_date: End date for processing
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.etl_logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            self.logger.info(f"Starting ETL process for date range {from_date} to {to_date}")
            
            # Step 1: Extract
            print("=== EXTRACT Phase ===")
            raw_data = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data is None or raw_data.count() == 0:
                raise Exception("Extraction failed or returned no data")
            
            # Step 2: Transform
            print("=== TRANSFORM Phase ===")
            analytics_data = self.transformer.transform_data(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise Exception("Transformation failed or returned no data")
            
            # Step 3: Load
            print("=== LOAD Phase ===")
            load_success = self.loader.load_data(analytics_data)
            
            if not load_success:
                raise Exception("Load failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.etl_logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )
            
            self.logger.info("ETL process completed successfully")
            
            return True
            
        except Exception as e:
            # Capture end time
            self.end_time = datetime.now()
            
            self.etl_logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id
    
    def display_summary(self):
        """Display ETL process summary."""
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        
        if self.start_time:
            print(f"Start Time:    {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.end_time:
            print(f"End Time:      {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)
    
    def cleanup(self):
        """Cleanup resources."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")