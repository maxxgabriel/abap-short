"""
ETL orchestration module for Sales ETL System.
Coordinates the execution of extract, transform, and load operations.
"""
from pyspark.sql import SparkSession
from datetime import datetime, date
from typing import Dict, Any
import logging

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.constants = ETLConstants()
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(spark, self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = DataExtractor(spark, self.logger)
        self.transformer = DataTransformer(spark, self.logger)
        self.loader = DataLoader(spark, self.logger)
        
        self.start_time = None
        self.end_time = None
        
        # Log initialization
        self.logger.log_message(
            step=self.constants.STEP_INIT,
            status=self.constants.STATUS_SUCCESS,
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique ETL run identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.constants.PREFIX_ETL_RUN}{timestamp}"
    
    def run_etl(
        self,
        from_date: date,
        to_date: date,
        source_path: str = None,
        target_path: str = None
    ) -> bool:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            source_path: Optional path to source data
            target_path: Optional path to target storage
        
        Returns:
            True if ETL completed successfully, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step=self.constants.STEP_INIT,
                status=self.constants.STATUS_INFO,
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_df, extract_success = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date,
                source_path=source_path
            )
            
            if not extract_success or raw_df.count() == 0:
                raise RuntimeError("Extraction failed or no data extracted")
            
            # Step 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_df, transform_success = self.transformer.transform_data(raw_df)
            
            if not transform_success:
                raise RuntimeError("Transformation failed")
            
            # Step 3: Load
            logging.info("=== LOAD Phase ===")
            loaded_count, load_success = self.loader.load_data(
                analytics_df=analytics_df,
                target_path=target_path
            )
            
            if not load_success:
                raise RuntimeError("Load failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step=self.constants.STEP_COMPLETE,
                status=self.constants.STATUS_SUCCESS,
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            return True
            
        except Exception as e:
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step=self.constants.STEP_ERROR,
                status=self.constants.STATUS_ERROR,
                message=f"ETL process failed: {str(e)}"
            )
            
            logging.error(f"ETL process failed: {str(e)}", exc_info=True)
            return False
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def display_summary(self) -> Dict[str, Any]:
        """
        Get ETL execution summary.
        
        Returns:
            Dictionary with execution summary
        """
        duration_seconds = 0
        if self.start_time and self.end_time:
            duration_seconds = int((self.end_time - self.start_time).total_seconds())
        
        summary = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration_seconds
        }
        
        # Print summary
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        print(f"Duration:      {summary['duration_seconds']} seconds")
        print("=" * 60)
        
        return summary
    
    def get_logs(self):
        """
        Get all logs from this ETL run.
        
        Returns:
            DataFrame with log entries
        """
        return self.logger.get_logs_dataframe()