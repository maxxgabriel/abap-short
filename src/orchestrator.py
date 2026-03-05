"""
ETL Orchestrator
Migrated from ZCL_ETL_ORCHESTRATOR
"""

from datetime import datetime
from typing import Dict, Optional
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.exceptions import ETLError


class ETLOrchestrator:
    """Main ETL orchestrator that coordinates the ETL process"""
    
    def __init__(self, config: Dict, test_mode: bool = False, log_level: str = 'INFO'):
        """
        Initialize ETL orchestrator
        
        Args:
            config: Configuration dictionary
            test_mode: Whether to run in test mode (no commits)
            log_level: Logging level
        """
        self.config = config
        self.test_mode = test_mode
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            log_level=log_level
        )
        
        # Initialize ETL components
        self.extractor = ETLExtractor(logger=self.logger, config=config)
        self.transformer = ETLTransformer(logger=self.logger, config=config)
        self.loader = ETLLoader(
            logger=self.logger,
            config=config,
            test_mode=test_mode
        )
        
        # Tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_error = 0
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
    
    def run_etl(self, from_date, to_date) -> bool:
        """
        Run complete ETL process
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time}'
            )
            
            # Phase 1: Extract
            print("=== EXTRACT Phase ===")
            raw_data = self._extract_phase(from_date, to_date)
            
            if not raw_data:
                raise ETLError("Extraction returned no data")
            
            # Phase 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_data = self._transform_phase(raw_data)
            
            if not analytics_data:
                raise ETLError("Transformation returned no data")
            
            # Phase 3: Load
            print("\n=== LOAD Phase ===")
            self._load_phase(analytics_data)
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time}'
            )
            
            return True
            
        except ETLError as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            logging.error(f"ETL Error: {e}", exc_info=True)
            return False
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'Unexpected error: {str(e)}'
            )
            logging.error(f"Unexpected error: {e}", exc_info=True)
            return False
    
    def _extract_phase(self, from_date, to_date):
        """Execute extraction phase"""
        raw_data = self.extractor.extract_data(
            from_date=from_date,
            to_date=to_date
        )
        
        self.records_extracted = len(raw_data)
        
        if self.records_extracted == 0:
            self.logger.log_message(
                step='EXTRACT',
                status='W',
                message='No records found in date range'
            )
        
        return raw_data
    
    def _transform_phase(self, raw_data):
        """Execute transformation phase"""
        analytics_data = self.transformer.transform_data(raw_data)
        
        self.records_transformed = len(analytics_data)
        
        return analytics_data
    
    def _load_phase(self, analytics_data):
        """Execute load phase"""
        success_count, error_count = self.loader.load_data(analytics_data)
        
        self.records_loaded = success_count
        self.records_error = error_count
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f'ETL{timestamp}'
    
    def get_summary(self) -> Dict:
        """
        Get execution summary
        
        Returns:
            Dict: Summary statistics
        """
        duration = 0
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration,
            'records_extracted': self.records_extracted,
            'records_transformed': self.records_transformed,
            'records_loaded': self.records_loaded,
            'records_error': self.records_error
        }