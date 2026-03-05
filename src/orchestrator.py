"""
ETL Orchestrator Module
Main pipeline orchestration with chained transformations, retry logic, and error handling
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Tuple, Optional, Dict, Any
import time
from datetime import datetime, timedelta
from functools import wraps

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger
from src.exceptions import ETLException, ExtractException, TransformException, LoadException
from src.config import ETLConfig


def retry_on_failure(max_attempts: int = 3, delay_seconds: int = 5):
    """Decorator for retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            last_exception = None
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts:
                        wait_time = delay_seconds * (2 ** (attempt - 1))
                        print(f"Attempt {attempt} failed: {str(e)}. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    attempt += 1
            
            raise last_exception
        return wrapper
    return decorator


class ETLOrchestrator:
    """Main orchestrator for ETL pipeline execution"""
    
    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        """
        Initialize ETL orchestrator
        
        Args:
            spark: SparkSession instance
            config_path: Path to configuration file
        """
        self.spark = spark
        self.config = ETLConfig(config_path)
        
        # Generate unique run ID
        self.etl_run_id = self._generate_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(self.spark, self.etl_run_id)
        
        # Initialize components
        self.extractor = DataExtractor(self.spark, self.logger, self.config)
        self.transformer = DataTransformer(self.spark, self.logger, self.config)
        self.loader = DataLoader(self.spark, self.logger, self.config)
        
        # Execution metadata
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict[str, Any] = {}
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    @retry_on_failure(max_attempts=3, delay_seconds=5)
    def _execute_extract(self, from_date: str, to_date: str) -> DataFrame:
        """
        Execute extraction phase with retry logic
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with raw sales data
            
        Raises:
            ExtractException: If extraction fails after retries
        """
        self.logger.log_message(
            step="EXTRACT",
            status="S",
            message=f"Starting extraction from {from_date} to {to_date}"
        )
        
        try:
            df = self.extractor.extract_data(from_date, to_date)
            
            record_count = df.count()
            self.statistics['extracted_count'] = record_count
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractException(f"Extraction phase failed: {str(e)}")
    
    @retry_on_failure(max_attempts=3, delay_seconds=5)
    def _execute_transform(self, raw_df: DataFrame) -> DataFrame:
        """
        Execute transformation phase with retry logic
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame
            
        Raises:
            TransformException: If transformation fails after retries
        """
        self.logger.log_message(
            step="TRANSFORM",
            status="S",
            message="Starting data transformation"
        )
        
        try:
            analytics_df = self.transformer.transform_data(raw_df, self.etl_run_id)
            
            record_count = analytics_df.count()
            self.statistics['transformed_count'] = record_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Transformed {record_count} records successfully"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformException(f"Transformation phase failed: {str(e)}")
    
    @retry_on_failure(max_attempts=3, delay_seconds=5)
    def _execute_load(self, analytics_df: DataFrame) -> int:
        """
        Execute load phase with retry logic
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Number of records loaded successfully
            
        Raises:
            LoadException: If load fails after retries
        """
        self.logger.log_message(
            step="LOAD",
            status="S",
            message="Starting data load"
        )
        
        try:
            loaded_count = self.loader.load_data(analytics_df)
            
            self.statistics['loaded_count'] = loaded_count
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=loaded_count,
                records_success=loaded_count,
                message=f"Loaded {loaded_count} records successfully"
            )
            
            return loaded_count
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise LoadException(f"Load phase failed: {str(e)}")
    
    def _validate_prerequisites(self) -> bool:
        """
        Validate all prerequisites before starting ETL
        
        Returns:
            True if all prerequisites are met
        """
        try:
            # Check Spark session
            if self.spark is None:
                raise ETLException("SparkSession is not initialized")
            
            # Validate components
            if not self.extractor.validate_prerequisites():
                raise ETLException("Extractor prerequisites validation failed")
            
            if not self.transformer.validate_prerequisites():
                raise ETLException("Transformer prerequisites validation failed")
            
            if not self.loader.validate_prerequisites():
                raise ETLException("Loader prerequisites validation failed")
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="VALIDATE",
                status="E",
                message=f"Prerequisites validation failed: {str(e)}"
            )
            return False
    
    def run_etl(self, from_date: str, to_date: str, validate: bool = True) -> bool:
        """
        Execute complete ETL pipeline with chained transformations
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            validate: Whether to validate prerequisites
            
        Returns:
            True if ETL completed successfully, False otherwise
        """
        success = False
        
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            # Validate prerequisites if requested
            if validate and not self._validate_prerequisites():
                raise ETLException("Prerequisites validation failed")
            
            # Phase 1: Extract
            print("\n=== EXTRACT Phase ===")
            raw_df = self._execute_extract(from_date, to_date)
            
            # Cache extracted data for performance
            raw_df = raw_df.cache()
            
            # Phase 2: Transform (chained transformations)
            print("\n=== TRANSFORM Phase ===")
            analytics_df = self._execute_transform(raw_df)
            
            # Cache transformed data
            analytics_df = analytics_df.cache()
            
            # Unpersist raw data to free memory
            raw_df.unpersist()
            
            # Phase 3: Load
            print("\n=== LOAD Phase ===")
            loaded_count = self._execute_load(analytics_df)
            
            # Unpersist analytics data
            analytics_df.unpersist()
            
            # Capture end time
            self.end_time = datetime.now()
            
            # Calculate duration
            duration = (self.end_time - self.start_time).total_seconds()
            self.statistics['duration_seconds'] = duration
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            success = True
            
        except ExtractException as e:
            self._handle_failure("EXTRACT", e)
        except TransformException as e:
            self._handle_failure("TRANSFORM", e)
        except LoadException as e:
            self._handle_failure("LOAD", e)
        except Exception as e:
            self._handle_failure("ERROR", e)
        finally:
            # Ensure end time is set
            if self.end_time is None:
                self.end_time = datetime.now()
        
        return success
    
    def _handle_failure(self, step: str, error: Exception):
        """
        Handle ETL failure with comprehensive error logging
        
        Args:
            step: ETL step where failure occurred
            error: Exception that caused the failure
        """
        self.end_time = datetime.now()
        
        error_message = f"ETL process failed at {step}: {str(error)}"
        
        self.logger.log_message(
            step="ERROR",
            status="E",
            message=error_message
        )
        
        print(f"\n*** {error_message} ***")
        
        # Log stack trace for debugging
        import traceback
        traceback.print_exc()
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID"""
        return self.etl_run_id
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get ETL execution statistics"""
        return {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.statistics.get('duration_seconds', 0),
            'extracted_count': self.statistics.get('extracted_count', 0),
            'transformed_count': self.statistics.get('transformed_count', 0),
            'loaded_count': self.statistics.get('loaded_count', 0)
        }
    
    def display_summary(self):
        """Display execution summary"""
        stats = self.get_statistics()
        
        print("\n" + "=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:       {stats['etl_run_id']}")
        print(f"Start Time:       {stats['start_time']}")
        print(f"End Time:         {stats['end_time']}")
        print(f"Duration:         {stats['duration_seconds']:.2f} seconds")
        print(f"Records Extracted: {stats['extracted_count']}")
        print(f"Records Transformed: {stats['transformed_count']}")
        print(f"Records Loaded:    {stats['loaded_count']}")
        print("=" * 70)
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            # Stop Spark session if needed
            # self.spark.stop()  # Only if orchestrator owns the session
            pass
        except Exception as e:
            print(f"Cleanup error: {str(e)}")