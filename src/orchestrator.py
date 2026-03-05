"""
ETL Orchestrator - Main ETL process coordinator
Coordinates extract, transform, and load phases
"""

from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Tuple

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.config import Config


class ETLOrchestrator:
    """Main ETL orchestrator coordinating all phases"""
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration object
        """
        self.spark = spark
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        
        # Generate ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(self.etl_run_id, config)
        
        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger, config)
        self.transformer = SalesTransformer(self.logger, config)
        self.loader = SalesLoader(self.logger, config)
        
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID
        
        Returns:
            Unique ETL run ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL process
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Success flag
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Phase 1: Extract
            self.log.info("=" * 60)
            self.log.info("EXTRACT Phase")
            self.log.info("=" * 60)
            
            raw_df, extract_success = self.extractor.extract_data(from_date, to_date)
            
            if not extract_success:
                raise RuntimeError("Extraction phase failed")
            
            # Phase 2: Transform
            self.log.info("=" * 60)
            self.log.info("TRANSFORM Phase")
            self.log.info("=" * 60)
            
            analytics_df, transform_success = self.transformer.transform_data(raw_df)
            
            if not transform_success:
                raise RuntimeError("Transformation phase failed")
            
            # Phase 3: Load
            self.log.info("=" * 60)
            self.log.info("LOAD Phase")
            self.log.info("=" * 60)
            
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise RuntimeError("Load phase failed")
            
            # Success
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            self.log.error(f"ETL process failed: {str(e)}", exc_info=True)
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID"""
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display ETL execution summary"""
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 70)