"""
ETL Orchestration Pipeline
Coordinates chained transformations with retry logic and comprehensive error handling
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional, Tuple, List, Callable
from datetime import datetime, timedelta
import logging
import time
from enum import Enum
import traceback

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader
from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import ETLError, ExtractError, TransformError, LoadError


class ETLStatus(Enum):
    """ETL execution status"""
    NEW = 'N'
    PROCESSING = 'P'
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'


class ETLStep(Enum):
    """ETL process steps"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class ETLOrchestrator:
    """
    Main ETL orchestrator coordinating extraction, transformation, and loading
    with retry logic and comprehensive error handling
    """
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id, config)
        
        # Initialize components
        self.extractor = SalesDataExtractor(spark, self.logger, config)
        self.transformer = SalesDataTransformer(spark, self.logger, config)
        self.loader = SalesDataLoader(spark, self.logger, config)
        
        # Execution metrics
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict[str, Any] = {}
        
        self.logger.log_message(
            step=ETLStep.INIT.value,
            status=ETLStatus.SUCCESS.value,
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"ETL{timestamp[:14]}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        test_mode: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute complete ETL pipeline with retry logic
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            test_mode: If True, run without committing data
        
        Returns:
            Tuple of (success flag, statistics dictionary)
        """
        self.start_time = datetime.now()
        success = False
        
        try:
            self.logger.log_message(
                step=ETLStep.INIT.value,
                status=ETLStatus.PROCESSING.value,
                message=f"ETL process started at {self.start_time}. Date range: {from_date} to {to_date}"
            )
            
            # Execute ETL chain
            raw_data = self._execute_with_retry(
                step=ETLStep.EXTRACT,
                func=self.extractor.extract_data,
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data is None or raw_data.rdd.isEmpty():
                raise ETLError("No data extracted from source")
            
            transformed_data = self._execute_with_retry(
                step=ETLStep.TRANSFORM,
                func=self.transformer.transform_data,
                raw_data=raw_data
            )
            
            if transformed_data is None or transformed_data.rdd.isEmpty():
                raise ETLError("Transformation produced no data")
            
            load_success = self._execute_with_retry(
                step=ETLStep.LOAD,
                func=self.loader.load_data,
                analytics_data=transformed_data,
                test_mode=test_mode
            )
            
            if not load_success:
                raise ETLError("Data load failed")
            
            # Mark as successful
            self.end_time = datetime.now()
            success = True
            
            # Collect statistics
            self._collect_statistics()
            
            self.logger.log_message(
                step=ETLStep.COMPLETE.value,
                status=ETLStatus.SUCCESS.value,
                message=f"ETL process completed successfully at {self.end_time}",
                records_processed=self.statistics.get('total_records', 0),
                records_success=self.statistics.get('success_records', 0)
            )
            
        except Exception as e:
            self.end_time = datetime.now()
            self._handle_pipeline_failure(e)
            success = False
        
        finally:
            # Always collect final statistics
            if not self.statistics:
                self._collect_statistics()
        
        return success, self.statistics
    
    def _execute_with_retry(
        self,
        step: ETLStep,
        func: Callable,
        **kwargs
    ) -> Any:
        """
        Execute a function with retry logic
        
        Args:
            step: ETL step being executed
            func: Function to execute
            **kwargs: Arguments to pass to function
        
        Returns:
            Function result
        
        Raises:
            ETLError: If all retry attempts fail
        """
        max_retries = self.config.retry_config.max_attempts
        retry_delay = self.config.retry_config.initial_delay
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.log_message(
                    step=step.value,
                    status=ETLStatus.PROCESSING.value,
                    message=f"Executing {step.value} (Attempt {attempt}/{max_retries})"
                )
                
                result = func(**kwargs)
                
                self.logger.log_message(
                    step=step.value,
                    status=ETLStatus.SUCCESS.value,
                    message=f"{step.value} completed successfully"
                )
                
                return result
                
            except Exception as e:
                error_msg = f"{step.value} attempt {attempt} failed: {str(e)}"
                
                if attempt < max_retries:
                    self.logger.log_message(
                        step=step.value,
                        status=ETLStatus.WARNING.value,
                        message=f"{error_msg}. Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= self.config.retry_config.backoff_multiplier
                else:
                    self.logger.log_message(
                        step=step.value,
                        status=ETLStatus.ERROR.value,
                        message=f"{error_msg}. Max retries exceeded."
                    )
                    raise self._map_exception_to_etl_error(step, e)
    
    def _map_exception_to_etl_error(self, step: ETLStep, error: Exception) -> ETLError:
        """Map generic exceptions to specific ETL errors"""
        error_mapping = {
            ETLStep.EXTRACT: ExtractError,
            ETLStep.TRANSFORM: TransformError,
            ETLStep.LOAD: LoadError
        }
        
        exception_class = error_mapping.get(step, ETLError)
        return exception_class(
            f"{step.value} failed: {str(error)}",
            step=step.value,
            original_error=error
        )
    
    def _handle_pipeline_failure(self, error: Exception):
        """
        Handle pipeline-level failures
        
        Args:
            error: Exception that caused failure
        """
        error_details = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc()
        }
        
        self.logger.log_message(
            step=ETLStep.ERROR.value,
            status=ETLStatus.ERROR.value,
            message=f"ETL pipeline failed: {error_details['error_message']}"
        )
        
        # Log detailed error if in debug mode
        if self.config.etl_config.get('debug_mode', False):
            self.logger.logger.error(f"Full traceback: {error_details['traceback']}")
        
        # Update statistics with error
        self.statistics['error'] = error_details
    
    def _collect_statistics(self):
        """Collect execution statistics"""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        self.statistics = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration,
            'total_records': self.extractor.statistics.get('records_extracted', 0),
            'success_records': self.loader.statistics.get('records_loaded', 0),
            'error_records': self.loader.statistics.get('records_failed', 0),
            'transform_errors': self.transformer.statistics.get('records_failed', 0)
        }
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID"""
        return self.etl_run_id
    
    def display_summary(self) -> Dict[str, Any]:
        """
        Display execution summary
        
        Returns:
            Summary dictionary
        """
        summary = {
            'ETL Run ID': self.etl_run_id,
            'Start Time': self.start_time.isoformat() if self.start_time else 'N/A',
            'End Time': self.end_time.isoformat() if self.end_time else 'N/A',
            'Duration (seconds)': self.statistics.get('duration_seconds', 0),
            'Total Records': self.statistics.get('total_records', 0),
            'Successfully Loaded': self.statistics.get('success_records', 0),
            'Failed Records': self.statistics.get('error_records', 0),
            'Transform Errors': self.statistics.get('transform_errors', 0)
        }
        
        # Log summary
        self.logger.logger.info("=" * 60)
        self.logger.logger.info("ETL Process Summary")
        self.logger.logger.info("=" * 60)
        for key, value in summary.items():
            self.logger.logger.info(f"{key}: {value}")
        self.logger.logger.info("=" * 60)
        
        return summary
    
    def validate_pipeline(self) -> Tuple[bool, List[str]]:
        """
        Validate pipeline prerequisites
        
        Returns:
            Tuple of (validation success, list of issues)
        """
        issues = []
        
        # Validate components
        if not self.extractor.validate_prerequisites():
            issues.append("Extractor prerequisites not met")
        
        if not self.transformer.validate_prerequisites():
            issues.append("Transformer prerequisites not met")
        
        if not self.loader.validate_prerequisites():
            issues.append("Loader prerequisites not met")
        
        # Validate configuration
        if not self.config.validate():
            issues.append("Invalid configuration")
        
        is_valid = len(issues) == 0
        
        if is_valid:
            self.logger.log_message(
                step=ETLStep.VALIDATE.value,
                status=ETLStatus.SUCCESS.value,
                message="Pipeline validation successful"
            )
        else:
            self.logger.log_message(
                step=ETLStep.VALIDATE.value,
                status=ETLStatus.ERROR.value,
                message=f"Pipeline validation failed: {', '.join(issues)}"
            )
        
        return is_valid, issues


def create_orchestrator(config_path: str = "config.yaml") -> ETLOrchestrator:
    """
    Factory function to create orchestrator instance
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        Configured ETLOrchestrator instance
    """
    spark = SparkSession.builder \
        .appName("SalesETLOrchestrator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    config = ETLConfig.from_yaml(config_path)
    
    return ETLOrchestrator(spark, config)