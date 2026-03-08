"""
ETL Orchestrator Module
Coordinates sequential execution of Extract -> Transform -> Load pipeline
with run management, timestamps, and duration metrics.
"""
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import logging
from pyspark.sql import SparkSession

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractionError, TransformationError, LoadError


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the complete ETL process.
    Generates unique run IDs, captures timestamps, and calculates duration metrics.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize ETL orchestrator with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
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
        
        # Timing attributes
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.phase_timings: Dict[str, float] = {}
        
        # Statistics
        self.statistics = {
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'errors_count': 0,
            'warnings_count': 0
        }
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
        logging.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID with timestamp.
        
        Returns:
            Unique ETL run ID (format: ETL_YYYYMMDD_HHMMSS_microseconds)
        """
        timestamp = datetime.now()
        run_id = f"ETL_{timestamp.strftime('%Y%m%d_%H%M%S')}_{timestamp.microsecond:06d}"
        return run_id
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID for this execution.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        test_mode: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline: Extract -> Transform -> Load.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            test_mode: If True, runs without committing data
            
        Returns:
            True if ETL process completed successfully, False otherwise
        """
        success = False
        
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            logging.info(f"{'='*60}")
            logging.info(f"Starting ETL Process")
            logging.info(f"Run ID: {self.etl_run_id}")
            logging.info(f"Date Range: {from_date} to {to_date}")
            logging.info(f"Test Mode: {test_mode}")
            logging.info(f"{'='*60}")
            
            # Phase 1: Extract
            extract_success, raw_df = self._execute_extract(from_date, to_date)
            if not extract_success:
                raise ExtractionError("Extraction phase failed")
            
            # Phase 2: Transform
            transform_success, analytics_df = self._execute_transform(raw_df)
            if not transform_success:
                raise TransformationError("Transformation phase failed")
            
            # Phase 3: Load
            load_success = self._execute_load(analytics_df, test_mode)
            if not load_success:
                raise LoadError("Load phase failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            # Log completion
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()} (Duration: {duration:.2f}s)'
            )
            
            logging.info(f"{'='*60}")
            logging.info("ETL Process Completed Successfully")
            logging.info(f"Duration: {duration:.2f} seconds")
            logging.info(f"{'='*60}")
            
            success = True
            
        except ExtractionError as e:
            self._handle_error('EXTRACT', str(e))
        except TransformationError as e:
            self._handle_error('TRANSFORM', str(e))
        except LoadError as e:
            self._handle_error('LOAD', str(e))
        except Exception as e:
            self._handle_error('ERROR', f"Unexpected error: {str(e)}")
        finally:
            # Ensure end time is set
            if self.end_time is None:
                self.end_time = datetime.now()
        
        return success
    
    def _execute_extract(
        self,
        from_date: str,
        to_date: str
    ) -> Tuple[bool, Optional[Any]]:
        """
        Execute extraction phase with timing.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            Tuple of (success flag, DataFrame or None)
        """
        logging.info(f"\n{'='*60}")
        logging.info("EXTRACT Phase")
        logging.info(f"{'='*60}")
        
        phase_start = datetime.now()
        
        try:
            raw_df = self.extractor.extract_data(from_date, to_date)
            
            # Update statistics
            record_count = raw_df.count()
            self.statistics['total_extracted'] = record_count
            
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['extract'] = phase_duration
            
            logging.info(f"Extract phase completed in {phase_duration:.2f}s")
            logging.info(f"Records extracted: {record_count}")
            
            return True, raw_df
            
        except Exception as e:
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['extract'] = phase_duration
            logging.error(f"Extract phase failed after {phase_duration:.2f}s: {str(e)}")
            return False, None
    
    def _execute_transform(self, raw_df: Any) -> Tuple[bool, Optional[Any]]:
        """
        Execute transformation phase with timing.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (success flag, DataFrame or None)
        """
        logging.info(f"\n{'='*60}")
        logging.info("TRANSFORM Phase")
        logging.info(f"{'='*60}")
        
        phase_start = datetime.now()
        
        try:
            analytics_df = self.transformer.transform_data(raw_df)
            
            # Update statistics
            record_count = analytics_df.count()
            self.statistics['total_transformed'] = record_count
            
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['transform'] = phase_duration
            
            logging.info(f"Transform phase completed in {phase_duration:.2f}s")
            logging.info(f"Records transformed: {record_count}")
            
            return True, analytics_df
            
        except Exception as e:
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['transform'] = phase_duration
            logging.error(f"Transform phase failed after {phase_duration:.2f}s: {str(e)}")
            return False, None
    
    def _execute_load(self, analytics_df: Any, test_mode: bool) -> bool:
        """
        Execute load phase with timing.
        
        Args:
            analytics_df: Analytics DataFrame to load
            test_mode: If True, skip actual database writes
            
        Returns:
            Success flag
        """
        logging.info(f"\n{'='*60}")
        logging.info("LOAD Phase")
        logging.info(f"{'='*60}")
        
        phase_start = datetime.now()
        
        try:
            success = self.loader.load_data(analytics_df, test_mode)
            
            if success:
                # Update statistics
                record_count = analytics_df.count()
                self.statistics['total_loaded'] = record_count
            
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['load'] = phase_duration
            
            logging.info(f"Load phase completed in {phase_duration:.2f}s")
            
            return success
            
        except Exception as e:
            phase_duration = (datetime.now() - phase_start).total_seconds()
            self.phase_timings['load'] = phase_duration
            logging.error(f"Load phase failed after {phase_duration:.2f}s: {str(e)}")
            return False
    
    def _handle_error(self, step: str, error_message: str):
        """
        Handle ETL error with logging.
        
        Args:
            step: ETL step where error occurred
            error_message: Error message
        """
        if self.end_time is None:
            self.end_time = datetime.now()
        
        self.statistics['errors_count'] += 1
        
        self.logger.log_message(
            step=step,
            status='E',
            message=f'ETL process failed: {error_message}'
        )
        
        logging.error(f"{'='*60}")
        logging.error(f"ETL Process Failed at {step}")
        logging.error(f"Error: {error_message}")
        logging.error(f"{'='*60}")
    
    def display_summary(self) -> Dict[str, Any]:
        """
        Generate and display execution summary with metrics.
        
        Returns:
            Dictionary containing execution summary
        """
        if self.start_time is None or self.end_time is None:
            logging.warning("Cannot generate summary - timing information incomplete")
            return {}
        
        duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': round(duration_seconds, 2),
            'phase_timings': {
                phase: round(duration, 2)
                for phase, duration in self.phase_timings.items()
            },
            'statistics': self.statistics.copy()
        }
        
        # Display summary
        logging.info(f"\n{'='*60}")
        logging.info("ETL Process Summary")
        logging.info(f"{'='*60}")
        logging.info(f"ETL Run ID:       {summary['etl_run_id']}")
        logging.info(f"Start Time:       {summary['start_time']}")
        logging.info(f"End Time:         {summary['end_time']}")
        logging.info(f"Duration:         {summary['duration_seconds']} seconds")
        logging.info(f"")
        logging.info("Phase Timings:")
        for phase, duration in summary['phase_timings'].items():
            logging.info(f"  {phase.capitalize():<12}: {duration:.2f}s")
        logging.info(f"")
        logging.info("Statistics:")
        logging.info(f"  Extracted:        {summary['statistics']['total_extracted']}")
        logging.info(f"  Transformed:      {summary['statistics']['total_transformed']}")
        logging.info(f"  Loaded:           {summary['statistics']['total_loaded']}")
        logging.info(f"  Errors:           {summary['statistics']['errors_count']}")
        logging.info(f"  Warnings:         {summary['statistics']['warnings_count']}")
        logging.info(f"{'='*60}")
        
        return summary
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get current ETL execution statistics.
        
        Returns:
            Dictionary of statistics
        """
        return self.statistics.copy()