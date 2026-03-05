"""
ETL Orchestrator
Migrated from ABAP ZCL_ETL_ORCHESTRATOR
Coordinates the ETL process flow
"""
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.utils.logger import ETLLogger
from src.utils.id_generator import generate_etl_run_id


class ETLOrchestrator:
    """Main ETL orchestrator that coordinates the ETL process"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], test_mode: bool = False):
        """
        Initialize ETL orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            test_mode: Whether to run in test mode
        """
        self.spark = spark
        self.config = config
        self.test_mode = test_mode
        
        # Generate unique ETL run ID
        self.etl_run_id = generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(etl_run_id=self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = Extractor(spark=spark, logger=self.logger, config=config)
        self.transformer = Transformer(spark=spark, logger=self.logger, config=config)
        self.loader = Loader(spark=spark, logger=self.logger, config=config)
        
        # Initialize tracking variables
        self.start_time = None
        self.end_time = None
        self.statistics = {}
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
    
    def run_etl(self, from_date: datetime, to_date: datetime) -> bool:
        """
        Execute the complete ETL process
        
        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            # Step 1: Extract
            self.logger.log_message(
                step='EXTRACT',
                status='I',
                message='=== EXTRACT Phase ==='
            )
            
            raw_data_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data_df is None or raw_data_df.count() == 0:
                raise Exception("Extraction failed or returned no data")
            
            self.statistics['extracted'] = raw_data_df.count()
            
            # Step 2: Transform
            self.logger.log_message(
                step='TRANSFORM',
                status='I',
                message='=== TRANSFORM Phase ==='
            )
            
            analytics_df = self.transformer.transform_data(
                raw_data_df=raw_data_df,
                etl_run_id=self.etl_run_id
            )
            
            if analytics_df is None or analytics_df.count() == 0:
                raise Exception("Transformation failed or returned no data")
            
            self.statistics['transformed'] = analytics_df.count()
            
            # Step 3: Load
            self.logger.log_message(
                step='LOAD',
                status='I',
                message='=== LOAD Phase ==='
            )
            
            load_success, load_stats = self.loader.load_data(
                analytics_df=analytics_df,
                test_mode=self.test_mode
            )
            
            if not load_success:
                raise Exception("Load failed")
            
            self.statistics['loaded'] = load_stats.get('success_count', 0)
            self.statistics['errors'] = load_stats.get('error_count', 0)
            self.statistics['warnings'] = load_stats.get('warning_count', 0)
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get ETL statistics
        
        Returns:
            Dictionary containing ETL statistics
        """
        return self.statistics
    
    def get_duration(self) -> float:
        """
        Get ETL duration in seconds
        
        Returns:
            Duration in seconds
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0