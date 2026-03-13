"""
Orchestrator module for Sales ETL process.
Coordinates the ETL workflow: Extract -> Transform -> Load.
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import uuid
from typing import Dict, Tuple

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader


class ETLOrchestrator:
    """Main orchestrator for the ETL process."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.extractor = SalesExtractor(spark, config)
        self.transformer = SalesTransformer(config, self.etl_run_id)
        self.loader = SalesLoader(config)
        
        # Statistics
        self.stats = {
            'total_extracted': 0,
            'total_transformed': 0,
            'total_loaded': 0,
            'start_time': None,
            'end_time': None
        }
        
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique run identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}_{unique_id}"
    
    def run_etl(self, from_date: str, to_date: str) -> Tuple[bool, Dict]:
        """
        Execute complete ETL process.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Tuple of (success flag, statistics dictionary)
        """
        self.logger.info(f"Starting ETL process with run ID: {self.etl_run_id}")
        self.stats['start_time'] = datetime.now()
        
        try:
            # Step 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(from_date, to_date)
            self.stats['total_extracted'] = raw_df.count()
            
            if self.stats['total_extracted'] == 0:
                self.logger.warning("No records extracted. ETL process completed with no data.")
                self.stats['end_time'] = datetime.now()
                return True, self.stats
            
            # Step 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df)
            self.stats['total_transformed'] = analytics_df.count()
            
            # Step 3: Load
            self.logger.info("=== LOAD Phase ===")
            loaded_count = self.loader.load_data(analytics_df)
            self.stats['total_loaded'] = loaded_count
            
            # Complete
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            self.logger.info(f"=== ETL Process Completed Successfully ===")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Records Extracted: {self.stats['total_extracted']}")
            self.logger.info(f"Records Transformed: {self.stats['total_transformed']}")
            self.logger.info(f"Records Loaded: {self.stats['total_loaded']}")
            
            return True, self.stats
            
        except Exception as e:
            self.stats['end_time'] = datetime.now()
            self.logger.error(f"ETL process failed: {str(e)}")
            return False, self.stats
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def display_summary(self) -> Dict:
        """
        Get ETL process summary.
        
        Returns:
            Dictionary containing process statistics
        """
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        else:
            duration = 0
            
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
            'duration_seconds': duration,
            'records_extracted': self.stats['total_extracted'],
            'records_transformed': self.stats['total_transformed'],
            'records_loaded': self.stats['total_loaded']
        }
        
        return summary