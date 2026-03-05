"""
Orchestrator Module - Sales ETL System
Main ETL orchestrator that coordinates the ETL process
"""
from datetime import date, datetime
from typing import Optional
from pyspark.sql import SparkSession
import logging

from src.logger import ETLLogger
from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.config import ETLConfig


class ETLOrchestrator:
    """Main ETL orchestrator coordinating extract, transform, load"""
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance
        """
        self.spark = spark
        self.config = config
        self.log = logging.getLogger(__name__)
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = ETLExtractor(spark, self.logger)
        self.transformer = ETLTransformer(spark, self.logger, config)
        self.loader = ETLLoader(spark, self.logger)
        
        # Track execution times
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(self, from_date: date, to_date: date) -> bool:
        """
        Execute complete ETL process
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            Success flag
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            self.log.info("=" * 60)
            self.log.info("=== EXTRACT Phase ===")
            self.log.info("=" * 60)
            
            # Step 1: Extract
            raw_df, extract_success = self.extractor.extract_data(from_date, to_date)
            
            if not extract_success:
                raise RuntimeError("Extraction failed")
            
            self.log.info("=" * 60)
            self.log.info("=== TRANSFORM Phase ===")
            self.log.info("=" * 60)
            
            # Step 2: Transform
            analytics_df, transform_success = self.transformer.transform_data(raw_df)
            
            if not transform_success:
                raise RuntimeError("Transformation failed")
            
            self.log.info("=" * 60)
            self.log.info("=== LOAD Phase ===")
            self.log.info("=" * 60)
            
            # Step 3: Load
            success_count, error_count, load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise RuntimeError("Load failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            self.log.info("=" * 60)
            self.log.info("ETL Process Completed Successfully")
            self.log.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.log.error(f"ETL process failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display ETL execution summary"""
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 70)
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        prefix = self.config.get("id_prefixes.etl_run", "ETL")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{prefix}{timestamp}"