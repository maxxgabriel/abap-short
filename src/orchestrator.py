"""
ETL Orchestrator - Main coordination class
Migrated from ABAP ZCL_ETL_ORCHESTRATOR
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
import logging

from src.config import ETLConfig
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.utils.id_generator import generate_etl_run_id


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    Replaces ZCL_ETL_ORCHESTRATOR.
    """
    
    def __init__(self, spark: SparkSession, config: ETLConfig, logger: logging.Logger):
        """
        Initialize ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
        # Generate unique ETL run ID
        self.etl_run_id = generate_etl_run_id()
        
        # Initialize ETL components
        self.extractor = Extractor(spark, config, logger, self.etl_run_id)
        self.transformer = Transformer(spark, config, logger, self.etl_run_id)
        self.loader = Loader(spark, config, logger, self.etl_run_id)
        
        # Timing
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.logger.info(
            f"ETL process initialized with run ID: {self.etl_run_id}",
            extra={'step': self.config.steps.INIT, 'status': self.config.status.SUCCESS}
        )
    
    def run_etl(self, from_date: datetime, to_date: datetime) -> bool:
        """
        Execute the complete ETL process.
        Replaces run_etl method from ABAP.
        
        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.info(
                f"ETL process started at {self.start_time.isoformat()}",
                extra={'step': self.config.steps.INIT, 'status': self.config.status.SUCCESS}
            )
            
            # Step 1: Extract
            print("=" * 70)
            print("=== EXTRACT Phase ===")
            print("=" * 70)
            
            raw_data = self.extractor.extract_data(from_date, to_date)
            
            if raw_data is None or raw_data.count() == 0:
                raise ValueError("Extraction failed or returned no data")
            
            # Step 2: Transform
            print("\n" + "=" * 70)
            print("=== TRANSFORM Phase ===")
            print("=" * 70)
            
            analytics_data = self.transformer.transform_data(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise ValueError("Transformation failed or returned no data")
            
            # Step 3: Load
            print("\n" + "=" * 70)
            print("=== LOAD Phase ===")
            print("=" * 70)
            
            load_success = self.loader.load_data(analytics_data)
            
            if not load_success:
                raise ValueError("Load failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.info(
                f"ETL process completed successfully at {self.end_time.isoformat()}",
                extra={'step': self.config.steps.COMPLETE, 'status': self.config.status.SUCCESS}
            )
            
            return True
            
        except Exception as ex:
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.error(
                f"ETL process failed: {str(ex)}",
                extra={'step': self.config.steps.ERROR, 'status': self.config.status.ERROR},
                exc_info=True
            )
            
            return False
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """
        Display ETL execution summary.
        Replaces display_summary method from ABAP.
        """
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time.isoformat() if self.start_time else 'N/A'}")
        print(f"End Time:      {self.end_time.isoformat() if self.end_time else 'N/A'}")
        
        # Calculate duration
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 70)