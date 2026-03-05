"""
ETL Orchestrator
Coordinates the complete ETL pipeline execution
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any
import logging

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        self.spark = spark
        self.config = config
        self.logger = ETLLogger(config)
        
        # Initialize ETL components
        self.extractor = SalesDataExtractor(spark, self.logger, config)
        self.transformer = SalesDataTransformer(self.logger, config)
        self.loader = SalesDataLoader(self.logger, config)
        
        self.start_time = None
        self.end_time = None
        
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL pipeline
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            True if ETL succeeds, False otherwise
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Phase 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(from_date, to_date)
            
            if raw_df is None or raw_df.count() == 0:
                raise ValueError("Extraction returned no data")
            
            # Phase 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df)
            
            if analytics_df is None or analytics_df.count() == 0:
                raise ValueError("Transformation returned no data")
            
            # Phase 3: Load
            logging.info("=== LOAD Phase ===")
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise ValueError("Load phase failed")
            
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            logging.error(f"ETL error: {str(e)}", exc_info=True)
            
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """Get ETL execution summary"""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            "etl_run_id": self.logger.etl_run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "log_file": self.logger.log_file
        }
    
    def display_summary(self) -> None:
        """Display ETL execution summary"""
        summary = self.get_summary()
        
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:       {summary['etl_run_id']}")
        print(f"Start Time:       {summary['start_time']}")
        print(f"End Time:         {summary['end_time']}")
        if summary['duration_seconds']:
            print(f"Duration:         {summary['duration_seconds']:.2f} seconds")
        print(f"Log File:         {summary['log_file']}")
        print("=" * 70)