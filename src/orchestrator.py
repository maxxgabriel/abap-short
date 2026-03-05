"""
ETL Orchestrator
Coordinates the complete ETL pipeline execution
"""
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.exceptions import ETLError, ExtractionError, TransformationError, LoadError


class ETLOrchestrator:
    """Main orchestrator for ETL pipeline execution"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        logger: ETLLogger,
        test_mode: bool = False
    ):
        """
        Initialize ETL orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
            test_mode: Whether to run in test mode
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.test_mode = test_mode
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.extractor = DataExtractor(spark, config, logger, self.etl_run_id)
        self.transformer = DataTransformer(spark, config, logger, self.etl_run_id)
        self.loader = DataLoader(spark, config, logger, self.etl_run_id, test_mode)
        
        # Execution tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict[str, int] = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': 0
        }
        
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL orchestrator initialized with run ID: {self.etl_run_id}'
        )
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f'ETL{timestamp}'
    
    def run_etl(self, from_date: datetime, to_date: datetime) -> bool:
        """
        Execute complete ETL pipeline
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time}'
            )
            
            # Phase 1: Extract
            print("=== EXTRACT Phase ===")
            raw_data = self._execute_extraction(from_date, to_date)
            
            if raw_data is None or raw_data.count() == 0:
                raise ExtractionError("No data extracted")
            
            self.statistics['extracted'] = raw_data.count()
            
            # Phase 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_data = self._execute_transformation(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise TransformationError("No data transformed")
            
            self.statistics['transformed'] = analytics_data.count()
            
            # Phase 3: Load
            print("\n=== LOAD Phase ===")
            load_success = self._execute_load(analytics_data)
            
            if not load_success:
                raise LoadError("Data load failed")
            
            self.statistics['loaded'] = analytics_data.count()
            
            # Complete
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time}'
            )
            
            return True
            
        except ETLError as e:
            self.end_time = datetime.now()
            self.statistics['errors'] += 1
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            return False
            
        except Exception as e:
            self.end_time = datetime.now()
            self.statistics['errors'] += 1
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'Unexpected error in ETL process: {str(e)}'
            )
            
            return False
    
    def _execute_extraction(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> Optional[DataFrame]:
        """Execute extraction phase"""
        try:
            return self.extractor.extract_data(from_date, to_date)
        except Exception as e:
            raise ExtractionError(f"Extraction failed: {str(e)}")
    
    def _execute_transformation(self, raw_data: DataFrame) -> Optional[DataFrame]:
        """Execute transformation phase"""
        try:
            return self.transformer.transform_data(raw_data)
        except Exception as e:
            raise TransformationError(f"Transformation failed: {str(e)}")
    
    def _execute_load(self, analytics_data: DataFrame) -> bool:
        """Execute load phase"""
        try:
            return self.loader.load_data(analytics_data)
        except Exception as e:
            raise LoadError(f"Load failed: {str(e)}")
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get execution summary
        
        Returns:
            Dictionary containing execution statistics
        """
        duration = 0
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration,
            **self.statistics
        }