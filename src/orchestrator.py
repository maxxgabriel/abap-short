"""
Orchestrator module for Sales ETL System.
Coordinates the overall ETL process across all components.
"""
from pyspark.sql import SparkSession
from datetime import datetime, date
from typing import Dict, Optional
import logging
import uuid

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, spark: SparkSession, config: Dict, logger: logging.Logger):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize components
        self.extractor = SalesExtractor(spark, logger)
        self.transformer = SalesTransformer(spark, logger, config)
        self.loader = SalesLoader(spark, logger)
        
        self.logger.info(f"ETL process initialized with run ID: {self.etl_run_id}")
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run identifier."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}_{str(uuid.uuid4())[:8]}"
    
    def run_etl(self, from_date: date, to_date: date, 
                source_path: Optional[str] = None,
                target_path: Optional[str] = None) -> bool:
        """
        Execute complete ETL process.
        
        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            source_path: Optional source data path
            target_path: Optional target data path
        
        Returns:
            Boolean indicating success/failure
        """
        try:
            self.start_time = datetime.now()
            self.logger.info(f"ETL process started at {self.start_time}")
            
            # Step 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            df_raw = self.extractor.extract_data(from_date, to_date, source_path)
            
            if df_raw.isEmpty():
                self.logger.warning("No data extracted. Aborting ETL process.")
                return False
            
            # Step 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            df_analytics = self.transformer.transform_data(df_raw, self.etl_run_id)
            
            if df_analytics.isEmpty():
                self.logger.warning("No data after transformation. Aborting ETL process.")
                return False
            
            # Step 3: Load
            self.logger.info("=== LOAD Phase ===")
            target = target_path or self.config['etl']['default_target_path']
            record_count = self.loader.load_data(df_analytics, target)
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.info(f"ETL process completed successfully at {self.end_time}")
            self.logger.info(f"Total duration: {duration} seconds")
            self.logger.info(f"Records processed: {record_count}")
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            return False
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run identifier."""
        return self.etl_run_id
    
    def display_summary(self) -> Dict:
        """
        Generate and return ETL execution summary.
        
        Returns:
            Dictionary with execution statistics
        """
        summary = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": None
        }
        
        if self.start_time and self.end_time:
            summary["duration_seconds"] = (self.end_time - self.start_time).total_seconds()
        
        self.logger.info("=" * 60)
        self.logger.info("ETL Process Summary")
        self.logger.info("=" * 60)
        for key, value in summary.items():
            self.logger.info(f"{key}: {value}")
        self.logger.info("=" * 60)
        
        return summary