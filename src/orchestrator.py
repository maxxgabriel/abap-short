"""
ETL Orchestrator Module - PySpark Implementation
Coordinates the complete ETL pipeline
"""

from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Dict

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.exceptions import ETLError


class ETLOrchestrator:
    """
    Main orchestrator for ETL pipeline
    Coordinates extraction, transformation, and loading
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize ETL Orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.log = logging.getLogger(__name__)
        
        # Generate ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.logger = ETLLogger(self.etl_run_id, spark)
        self.extractor = ETLExtractor(spark, self.logger, config)
        self.transformer = ETLTransformer(spark, self.logger, config)
        self.loader = ETLLoader(spark, self.logger, config)
        
        # Track execution
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL pipeline
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            
        Returns:
            Success flag
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            self.log.info("=== EXTRACT Phase ===")
            raw_df, extract_success = self.extractor.extract_data(from_date, to_date)
            
            if not extract_success:
                raise ETLError("Extraction failed", error_step='EXTRACT')
            
            # Step 2: Transform
            self.log.info("=== TRANSFORM Phase ===")
            analytics_df, transform_success = self.transformer.transform_data(raw_df)
            
            if not transform_success:
                raise ETLError("Transformation failed", error_step='TRANSFORM')
            
            # Step 3: Load
            self.log.info("=== LOAD Phase ===")
            load_success, load_stats = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise ETLError("Load failed", error_step='LOAD')
            
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            # Display summary
            self._display_summary()
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f"ETL process failed: {str(e)}"
            )
            
            self.log.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            return False
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID"""
        return self.etl_run_id
    
    def _display_summary(self) -> None:
        """Display ETL execution summary"""
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            
            summary = f"""
{'=' * 60}
ETL Process Summary
{'=' * 60}
ETL Run ID:    {self.etl_run_id}
Start Time:    {self.start_time}
End Time:      {self.end_time}
Duration:      {duration:.2f} seconds
{'=' * 60}
            """
            
            self.log.info(summary)
            print(summary)