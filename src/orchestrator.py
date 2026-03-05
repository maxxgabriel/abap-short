"""
ETL Orchestrator Module
Coordinates the complete ETL process with exception handling and UUID-based run ID generation.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractError, TransformError, LoadError
from src.config import Config


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the complete ETL pipeline.
    
    Attributes:
        config: Configuration object
        etl_run_id: Unique UUID for this ETL run
        logger: ETL logger instance
        extractor: Data extraction component
        transformer: Data transformation component
        loader: Data loading component
        start_time: Process start timestamp
        end_time: Process end timestamp
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Optional configuration object (creates default if None)
        """
        self.spark = spark
        self.config = config or Config()
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            log_level=self.config.log_level
        )
        
        # Initialize ETL components
        self.extractor = Extractor(spark=spark, logger=self.logger)
        self.transformer = Transformer(config=self.config, logger=self.logger)
        self.loader = Loader(spark=spark, logger=self.logger)
        
        # Timing attributes
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate a unique ETL run ID using UUID.
        
        Returns:
            Unique run ID with ETL prefix
        """
        unique_id = str(uuid.uuid4()).replace("-", "")[:14].upper()
        return f"ETL{unique_id}"
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date for extraction (format: YYYY-MM-DD)
            to_date: End date for extraction (format: YYYY-MM-DD)
            
        Returns:
            True if ETL process completed successfully, False otherwise
            
        Raises:
            ETLError: If a fatal error occurs during processing
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            # Step 1: Extract
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            raw_data = self._execute_extract(from_date, to_date)
            
            if raw_data is None or raw_data.count() == 0:
                raise ExtractError(
                    error_text="No data extracted for the given date range",
                    error_step="EXTRACT"
                )
            
            # Step 2: Transform
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            analytics_data = self._execute_transform(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise TransformError(
                    error_text="No data produced from transformation",
                    error_step="TRANSFORM"
                )
            
            # Step 3: Load
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            self._execute_load(analytics_data)
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except ExtractError as e:
            self._handle_error(e, "EXTRACT")
            return False
            
        except TransformError as e:
            self._handle_error(e, "TRANSFORM")
            return False
            
        except LoadError as e:
            self._handle_error(e, "LOAD")
            return False
            
        except Exception as e:
            self._handle_error(
                ETLError(error_text=str(e), error_step="UNKNOWN"),
                "UNKNOWN"
            )
            return False
    
    def _execute_extract(self, from_date: str, to_date: str) -> DataFrame:
        """
        Execute the extraction phase.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame with extracted raw data
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            return self.extractor.extract_data(from_date, to_date)
        except Exception as e:
            raise ExtractError(
                error_text=f"Extraction failed: {str(e)}",
                error_step="EXTRACT"
            ) from e
    
    def _execute_transform(self, raw_data: DataFrame) -> DataFrame:
        """
        Execute the transformation phase.
        
        Args:
            raw_data: Raw data DataFrame
            
        Returns:
            DataFrame with transformed analytics data
            
        Raises:
            TransformError: If transformation fails
        """
        try:
            return self.transformer.transform_data(raw_data, self.etl_run_id)
        except Exception as e:
            raise TransformError(
                error_text=f"Transformation failed: {str(e)}",
                error_step="TRANSFORM"
            ) from e
    
    def _execute_load(self, analytics_data: DataFrame) -> None:
        """
        Execute the loading phase.
        
        Args:
            analytics_data: Transformed analytics data
            
        Raises:
            LoadError: If loading fails
        """
        try:
            self.loader.load_data(analytics_data)
        except Exception as e:
            raise LoadError(
                error_text=f"Load failed: {str(e)}",
                error_step="LOAD"
            ) from e
    
    def _handle_error(self, error: ETLError, step: str) -> None:
        """
        Handle ETL errors with logging.
        
        Args:
            error: The ETL error that occurred
            step: The step where the error occurred
        """
        if self.end_time is None:
            self.end_time = datetime.now()
        
        self.logger.log_message(
            step="ERROR",
            status="E",
            message=f"ETL process failed at {step}: {error.error_text}"
        )
        
        logging.error(f"ETL Error in {step}: {error.error_text}")
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            The unique ETL run ID
        """
        return self.etl_run_id
    
    def display_summary(self) -> Dict[str, Any]:
        """
        Generate and display ETL process summary.
        
        Returns:
            Dictionary containing summary statistics
        """
        summary = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": None,
            "status": "COMPLETED" if self.end_time else "IN_PROGRESS"
        }
        
        # Calculate duration
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            summary["duration_seconds"] = duration
        
        # Print summary
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        if summary['duration_seconds'] is not None:
            print(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        print(f"Status:        {summary['status']}")
        print("=" * 60)
        
        return summary
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get detailed statistics from the logger.
        
        Returns:
            Dictionary with ETL statistics
        """
        return self.logger.get_statistics()