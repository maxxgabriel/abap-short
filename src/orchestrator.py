"""
ETL Orchestrator Module
Coordinates the sequential execution of Extract, Transform, and Load operations
with comprehensive error propagation and timestamp tracking.
"""

from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import logging

from pyspark.sql import SparkSession, DataFrame

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractionError, TransformationError, LoadError


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
        etl_run_id: Unique identifier for this ETL run
        start_time: ETL process start timestamp
        end_time: ETL process end timestamp
        extractor: Data extraction component
        transformer: Data transformation component
        loader: Data loading component
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary containing ETL parameters
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize logger
        self.logger = ETLLogger(
            spark=spark,
            etl_run_id=self.etl_run_id,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger, config)
        self.transformer = SalesTransformer(spark, self.logger, config)
        self.loader = SalesLoader(spark, self.logger, config)
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process with error propagation.
        
        Args:
            from_date: Start date for data extraction (YYYY-MM-DD)
            to_date: End date for data extraction (YYYY-MM-DD)
        
        Returns:
            bool: True if ETL completed successfully, False otherwise
        
        Raises:
            ETLError: If any critical error occurs during ETL process
        """
        extract_success = False
        transform_success = False
        load_success = False
        raw_data: Optional[DataFrame] = None
        analytics_data: Optional[DataFrame] = None
        
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            # Step 1: Extract
            print("=== EXTRACT Phase ===")
            raw_data, extract_success = self._execute_extract(from_date, to_date)
            
            if not extract_success or raw_data is None:
                raise ExtractionError(
                    error_text="Extraction phase failed",
                    error_step="EXTRACT"
                )
            
            # Step 2: Transform
            print("=== TRANSFORM Phase ===")
            analytics_data, transform_success = self._execute_transform(raw_data)
            
            if not transform_success or analytics_data is None:
                raise TransformationError(
                    error_text="Transformation phase failed",
                    error_step="TRANSFORM"
                )
            
            # Step 3: Load
            print("=== LOAD Phase ===")
            load_success = self._execute_load(analytics_data)
            
            if not load_success:
                raise LoadError(
                    error_text="Load phase failed",
                    error_step="LOAD"
                )
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except (ExtractionError, TransformationError, LoadError) as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            # Re-raise to allow caller to handle
            raise
            
        except Exception as e:
            self.end_time = datetime.now()
            
            error_msg = f"Unexpected error in ETL process: {str(e)}"
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=error_msg
            )
            
            raise ETLError(
                error_text=error_msg,
                error_step="ORCHESTRATOR"
            ) from e
    
    def _execute_extract(self, from_date: str, to_date: str) -> Tuple[Optional[DataFrame], bool]:
        """
        Execute the extraction phase with error handling.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
        
        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        try:
            raw_data = self.extractor.extract_data(from_date, to_date)
            
            if raw_data is None:
                return None, False
            
            return raw_data, True
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extract execution failed: {str(e)}"
            )
            return None, False
    
    def _execute_transform(self, raw_data: DataFrame) -> Tuple[Optional[DataFrame], bool]:
        """
        Execute the transformation phase with error handling.
        
        Args:
            raw_data: Raw sales data DataFrame
        
        Returns:
            Tuple of (DataFrame or None, success boolean)
        """
        try:
            analytics_data = self.transformer.transform_data(raw_data)
            
            if analytics_data is None:
                return None, False
            
            return analytics_data, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transform execution failed: {str(e)}"
            )
            return None, False
    
    def _execute_load(self, analytics_data: DataFrame) -> bool:
        """
        Execute the load phase with error handling.
        
        Args:
            analytics_data: Transformed analytics data DataFrame
        
        Returns:
            bool: True if load successful, False otherwise
        """
        try:
            return self.loader.load_data(analytics_data)
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load execution failed: {str(e)}"
            )
            return False
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate a unique ETL run identifier.
        
        Returns:
            str: Unique ETL run ID with format ETL_YYYYMMDDHHMMSS
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL_{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id
    
    def display_summary(self) -> Dict[str, Any]:
        """
        Display and return ETL execution summary.
        
        Returns:
            Dict containing summary statistics
        """
        summary = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": None
        }
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            summary["duration_seconds"] = duration
        
        # Print summary
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        if summary['duration_seconds'] is not None:
            print(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        print("=" * 60)
        
        return summary
    
    def cleanup(self):
        """Clean up resources and close connections."""
        try:
            self.logger.log_message(
                step="CLEANUP",
                status="I",
                message="Cleaning up ETL resources"
            )
            
            # Persist logs if configured
            if self.config.get("persist_logs", True):
                self.logger.persist_logs()
            
        except Exception as e:
            logging.warning(f"Error during cleanup: {str(e)}")