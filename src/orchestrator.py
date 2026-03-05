"""
ETL Orchestrator Module
Coordinates the ETL process for sales data migration from ABAP to Python/PySpark
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import logging

from pyspark.sql import SparkSession

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractError, TransformError, LoadError
from src.config import ETLConfig


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the complete ETL process.
    
    Responsibilities:
    - Initialize all ETL components
    - Generate unique ETL run IDs using UUID
    - Coordinate extract, transform, and load phases
    - Handle exceptions and logging
    - Track execution metrics
    """
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration object
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID using UUID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize execution tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize logger
        self.logger = ETLLogger(etl_run_id=self.etl_run_id, config=config)
        
        # Initialize ETL components
        self.extractor = SalesDataExtractor(spark=spark, logger=self.logger, config=config)
        self.transformer = SalesDataTransformer(spark=spark, logger=self.logger, config=config)
        self.loader = SalesDataLoader(spark=spark, logger=self.logger, config=config)
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID using UUID4.
        
        Returns:
            Unique ETL run ID string (format: ETL-<uuid>)
        """
        unique_id = str(uuid.uuid4())
        return f"ETL-{unique_id}"
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date for extraction (format: YYYY-MM-DD)
            to_date: End date for extraction (format: YYYY-MM-DD)
            
        Returns:
            True if ETL process completed successfully, False otherwise
            
        Raises:
            ETLError: If critical error occurs during ETL execution
        """
        try:
            # Capture start time
            self.start_time = datetime.now(timezone.utc)
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            # Step 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_data_df = self._execute_extract(from_date, to_date)
            
            # Step 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_df = self._execute_transform(raw_data_df)
            
            # Step 3: Load
            logging.info("=== LOAD Phase ===")
            self._execute_load(analytics_df)
            
            # Capture end time
            self.end_time = datetime.now(timezone.utc)
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except ExtractError as e:
            self._handle_error("EXTRACT", e)
            return False
            
        except TransformError as e:
            self._handle_error("TRANSFORM", e)
            return False
            
        except LoadError as e:
            self._handle_error("LOAD", e)
            return False
            
        except Exception as e:
            self._handle_error("ERROR", ETLError(f"Unexpected error: {str(e)}"))
            return False
    
    def _execute_extract(self, from_date: str, to_date: str):
        """
        Execute data extraction phase.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame containing extracted raw sales data
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            raw_data_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data_df is None or raw_data_df.count() == 0:
                raise ExtractError("No data extracted from source")
            
            return raw_data_df
            
        except Exception as e:
            raise ExtractError(f"Extraction failed: {str(e)}")
    
    def _execute_transform(self, raw_data_df):
        """
        Execute data transformation phase.
        
        Args:
            raw_data_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
            
        Raises:
            TransformError: If transformation fails
        """
        try:
            analytics_df = self.transformer.transform_data(raw_data_df)
            
            if analytics_df is None or analytics_df.count() == 0:
                raise TransformError("Transformation produced no results")
            
            return analytics_df
            
        except Exception as e:
            raise TransformError(f"Transformation failed: {str(e)}")
    
    def _execute_load(self, analytics_df):
        """
        Execute data loading phase.
        
        Args:
            analytics_df: DataFrame containing analytics data to load
            
        Raises:
            LoadError: If loading fails
        """
        try:
            success = self.loader.load_data(analytics_df)
            
            if not success:
                raise LoadError("Data loading completed with errors")
                
        except Exception as e:
            raise LoadError(f"Load failed: {str(e)}")
    
    def _handle_error(self, step: str, error: Exception):
        """
        Handle ETL errors with proper logging.
        
        Args:
            step: ETL step where error occurred
            error: Exception that was raised
        """
        self.end_time = datetime.now(timezone.utc)
        
        self.logger.log_message(
            step=step,
            status="E",
            message=f"ETL process failed: {str(error)}"
        )
        
        logging.error(f"ETL Error in {step}: {str(error)}", exc_info=True)
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Get execution summary with metrics.
        
        Returns:
            Dictionary containing execution metrics
        """
        duration_seconds = None
        if self.start_time and self.end_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        return {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration_seconds,
            "status": "COMPLETED" if self.end_time else "RUNNING"
        }
    
    def display_summary(self):
        """
        Display execution summary to console.
        """
        summary = self.get_execution_summary()
        
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        
        if summary['duration_seconds']:
            print(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        
        print("=" * 60)