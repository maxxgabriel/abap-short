"""
ETL Orchestrator Module

Orchestrates the sequential execution of Extract, Transform, and Load phases
with comprehensive error handling and timestamp tracking.
"""
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractionError, TransformationError, LoadError


class ETLOrchestrator:
    """
    Main orchestrator that coordinates the ETL process flow.
    
    Manages sequential execution of:
    1. Extract phase
    2. Transform phase
    3. Load phase
    
    Tracks timestamps and propagates errors with proper context.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ETL orchestrator with configuration.
        
        Args:
            config: Configuration dictionary containing ETL parameters
        """
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id, config)
        
        # Initialize ETL components
        self.extractor = DataExtractor(self.logger, config)
        self.transformer = DataTransformer(self.logger, config)
        self.loader = DataLoader(self.logger, config)
        
        # Timing tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Statistics
        self.statistics = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': 0
        }
        
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date for data extraction (YYYY-MM-DD)
            to_date: End date for data extraction (YYYY-MM-DD)
            
        Returns:
            bool: True if ETL completed successfully, False otherwise
            
        Raises:
            ETLError: If any phase fails with unrecoverable error
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
            
            # Phase 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_data = self._execute_extract_phase(from_date, to_date)
            
            if raw_data is None or raw_data.count() == 0:
                raise ExtractionError("No data extracted from source")
            
            self.statistics['extracted'] = raw_data.count()
            
            # Phase 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_data = self._execute_transform_phase(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise TransformationError("No data produced from transformation")
            
            self.statistics['transformed'] = analytics_data.count()
            
            # Phase 3: Load
            logging.info("=== LOAD Phase ===")
            load_count = self._execute_load_phase(analytics_data)
            
            self.statistics['loaded'] = load_count
            
            # Capture end time
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                records_processed=self.statistics['extracted'],
                records_success=self.statistics['loaded'],
                message=f'ETL process completed successfully in {duration:.2f} seconds'
            )
            
            success = True
            
        except ExtractionError as e:
            self._handle_phase_error('EXTRACT', e)
            raise
            
        except TransformationError as e:
            self._handle_phase_error('TRANSFORM', e)
            raise
            
        except LoadError as e:
            self._handle_phase_error('LOAD', e)
            raise
            
        except Exception as e:
            self._handle_phase_error('ERROR', e)
            raise ETLError(f"Unexpected error in ETL process: {str(e)}") from e
            
        finally:
            # Ensure end time is set
            if self.end_time is None:
                self.end_time = datetime.now()
        
        return success
    
    def _execute_extract_phase(self, from_date: str, to_date: str):
        """
        Execute the extraction phase.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame: Extracted raw data
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            raw_data = self.extractor.extract_data(from_date, to_date)
            
            if raw_data is None:
                raise ExtractionError("Extractor returned None")
            
            count = raw_data.count()
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Successfully extracted {count} records'
            )
            
            return raw_data
            
        except Exception as e:
            raise ExtractionError(f"Extraction phase failed: {str(e)}") from e
    
    def _execute_transform_phase(self, raw_data):
        """
        Execute the transformation phase.
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            DataFrame: Transformed analytics data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            analytics_data = self.transformer.transform_data(raw_data)
            
            if analytics_data is None:
                raise TransformationError("Transformer returned None")
            
            count = analytics_data.count()
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Successfully transformed {count} records'
            )
            
            return analytics_data
            
        except Exception as e:
            raise TransformationError(f"Transformation phase failed: {str(e)}") from e
    
    def _execute_load_phase(self, analytics_data) -> int:
        """
        Execute the load phase.
        
        Args:
            analytics_data: DataFrame with analytics data
            
        Returns:
            int: Number of records loaded
            
        Raises:
            LoadError: If load fails
        """
        try:
            load_count = self.loader.load_data(analytics_data)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=load_count,
                records_success=load_count,
                message=f'Successfully loaded {load_count} records'
            )
            
            return load_count
            
        except Exception as e:
            raise LoadError(f"Load phase failed: {str(e)}") from e
    
    def _handle_phase_error(self, phase: str, error: Exception):
        """
        Handle errors from ETL phases with proper logging.
        
        Args:
            phase: Name of the phase that failed
            error: Exception that occurred
        """
        self.end_time = datetime.now()
        self.statistics['errors'] += 1
        
        error_msg = f"{phase} phase failed: {str(error)}"
        
        self.logger.log_message(
            step='ERROR',
            status='E',
            message=error_msg
        )
        
        logging.error(error_msg, exc_info=True)
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            str: Unique ETL run ID with timestamp
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get ETL execution statistics.
        
        Returns:
            dict: Statistics including counts and timing
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration,
            'records_extracted': self.statistics['extracted'],
            'records_transformed': self.statistics['transformed'],
            'records_loaded': self.statistics['loaded'],
            'errors': self.statistics['errors']
        }
    
    def display_summary(self):
        """
        Display execution summary to console.
        """
        stats = self.get_statistics()
        
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:         {stats['etl_run_id']}")
        print(f"Start Time:         {stats['start_time']}")
        print(f"End Time:           {stats['end_time']}")
        
        if stats['duration_seconds']:
            print(f"Duration:           {stats['duration_seconds']:.2f} seconds")
        
        print(f"Records Extracted:  {stats['records_extracted']}")
        print(f"Records Transformed: {stats['records_transformed']}")
        print(f"Records Loaded:     {stats['records_loaded']}")
        print(f"Errors:             {stats['errors']}")
        print("=" * 70)