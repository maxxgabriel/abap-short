"""
ETL orchestrator module.
Coordinates the complete ETL pipeline workflow.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional, Dict
import logging
import uuid

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = self._setup_logger()
        
        # Initialize components
        self.extractor = DataExtractor(spark, config, self.logger)
        self.transformer = DataTransformer(spark, config, self.logger, self.etl_run_id)
        self.loader = DataLoader(spark, config, self.logger)
        
        # Track statistics
        self.stats = {
            "etl_run_id": self.etl_run_id,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "extracted_records": 0,
            "transformed_records": 0,
            "loaded_records": 0,
            "failed_records": 0,
            "status": "INITIALIZED"
        }
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}_{unique_id}"
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        
        return logger
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Run the complete ETL pipeline.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            True if ETL succeeds, False otherwise
        """
        try:
            self.stats["start_time"] = datetime.now()
            self.logger.info(f"Starting ETL run: {self.etl_run_id}")
            self.logger.info(f"Date range: {from_date} to {to_date}")
            
            # Step 1: Extract
            self.logger.info("=" * 60)
            self.logger.info("EXTRACT Phase")
            self.logger.info("=" * 60)
            
            raw_df = self.extractor.extract_data(from_date, to_date)
            if raw_df is None or raw_df.rdd.isEmpty():
                self.logger.warning("No data extracted")
                self.stats["status"] = "NO_DATA"
                return False
            
            self.stats["extracted_records"] = raw_df.count()
            self.logger.info(f"Extracted {self.stats['extracted_records']} records")
            
            # Step 2: Transform
            self.logger.info("=" * 60)
            self.logger.info("TRANSFORM Phase")
            self.logger.info("=" * 60)
            
            analytics_df = self.transformer.transform_data(raw_df)
            if analytics_df is None:
                raise Exception("Transformation returned None")
            
            # Validate transformed data
            if not self.transformer.validate_transformed_data(analytics_df):
                raise Exception("Transformed data validation failed")
            
            self.stats["transformed_records"] = analytics_df.count()
            self.logger.info(f"Transformed {self.stats['transformed_records']} records")
            
            # Step 3: Load
            self.logger.info("=" * 60)
            self.logger.info("LOAD Phase")
            self.logger.info("=" * 60)
            
            load_success = self.loader.load_data(analytics_df)
            if not load_success:
                raise Exception("Data load failed")
            
            self.stats["loaded_records"] = analytics_df.count()
            
            # Get load statistics
            load_stats = self.loader.get_load_statistics(analytics_df)
            self.stats.update(load_stats)
            
            # Update source status
            trans_ids = [row.trans_id for row in raw_df.select("trans_id").collect()]
            self.loader.update_source_status(trans_ids)
            
            # Calculate final statistics
            self.stats["end_time"] = datetime.now()
            self.stats["duration_seconds"] = (
                self.stats["end_time"] - self.stats["start_time"]
            ).total_seconds()
            self.stats["status"] = "SUCCESS"
            
            self.logger.info("=" * 60)
            self.logger.info("ETL Process Completed Successfully")
            self.logger.info("=" * 60)
            self.display_summary()
            
            return True
            
        except Exception as e:
            self.stats["end_time"] = datetime.now()
            self.stats["status"] = "FAILED"
            self.logger.error(f"ETL process failed: {str(e)}")
            self.display_summary()
            return False
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def get_statistics(self) -> Dict:
        """Get ETL execution statistics."""
        return self.stats
    
    def display_summary(self):
        """Display ETL execution summary."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("ETL Execution Summary")
        self.logger.info("=" * 60)
        self.logger.info(f"ETL Run ID:         {self.stats['etl_run_id']}")
        self.logger.info(f"Status:             {self.stats['status']}")
        self.logger.info(f"Start Time:         {self.stats['start_time']}")
        self.logger.info(f"End Time:           {self.stats['end_time']}")
        
        if self.stats['duration_seconds'] is not None:
            self.logger.info(f"Duration:           {self.stats['duration_seconds']:.2f} seconds")
        
        self.logger.info(f"Extracted Records:  {self.stats['extracted_records']}")
        self.logger.info(f"Transformed Records:{self.stats['transformed_records']}")
        self.logger.info(f"Loaded Records:     {self.stats['loaded_records']}")
        self.logger.info(f"Failed Records:     {self.stats['failed_records']}")
        
        if "high_value_sales" in self.stats:
            self.logger.info("\nCategory Breakdown:")
            self.logger.info(f"  High Value:       {self.stats.get('high_value_sales', 0)}")
            self.logger.info(f"  Medium Value:     {self.stats.get('medium_value_sales', 0)}")
            self.logger.info(f"  Low Value:        {self.stats.get('low_value_sales', 0)}")
        
        self.logger.info("=" * 60 + "\n")