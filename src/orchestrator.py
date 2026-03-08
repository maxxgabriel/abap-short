"""
ETL Orchestrator - Main coordination module
Orchestrates the complete ETL pipeline
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Dict, Optional
import uuid

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader
from src.logger import ETLLogger

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize orchestrator
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize logger
        self.logger = ETLLogger(spark, self.etl_run_id, config)
        
        # Initialize ETL components
        self.extractor = SalesDataExtractor(spark, config)
        self.transformer = SalesDataTransformer(spark, config)
        self.loader = SalesDataLoader(spark, config)
    
    @staticmethod
    def _generate_etl_run_id() -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL_{timestamp}_{str(uuid.uuid4())[:8]}"
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        use_sample_data: bool = False
    ) -> Dict[str, any]:
        """
        Execute complete ETL pipeline
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            use_sample_data: Use sample data instead of reading from source
            
        Returns:
            Dictionary with execution results
        """
        self.start_time = datetime.now()
        
        self.logger.log_message(
            step="START",
            status="S",
            message=f"ETL process started at {self.start_time}"
        )
        
        results = {
            "etl_run_id": self.etl_run_id,
            "success": False,
            "start_time": self.start_time,
            "end_time": None,
            "duration_seconds": None,
            "statistics": {}
        }
        
        try:
            # Phase 1: Extract
            logger.info("=== EXTRACT Phase ===")
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            if use_sample_data:
                df_raw = self.extractor.create_sample_data()
            else:
                df_raw = self.extractor.extract_data(
                    from_date, 
                    to_date, 
                    self.etl_run_id
                )
            
            extract_count = df_raw.count()
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=extract_count,
                records_success=extract_count,
                message=f"Extracted {extract_count} records"
            )
            
            results["statistics"]["extracted"] = extract_count
            
            # Phase 2: Transform
            logger.info("=== TRANSFORM Phase ===")
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            df_analytics = self.transformer.transform_data(
                df_raw, 
                self.etl_run_id
            )
            
            transform_count = df_analytics.count()
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=transform_count,
                records_success=transform_count,
                message=f"Transformed {transform_count} records"
            )
            
            results["statistics"]["transformed"] = transform_count
            
            # Phase 3: Load
            logger.info("=== LOAD Phase ===")
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            load_success, load_stats = self.loader.load_data(
                df_analytics,
                self.etl_run_id
            )
            
            self.logger.log_message(
                step="LOAD",
                status="S" if load_success else "E",
                records_processed=load_stats["total_count"],
                records_success=load_stats["valid_count"],
                records_error=load_stats["invalid_count"],
                message=f"Loaded {load_stats['valid_count']} records"
            )
            
            results["statistics"]["loaded"] = load_stats["valid_count"]
            results["statistics"]["load_errors"] = load_stats["invalid_count"]
            
            # Completion
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            results["success"] = load_success
            results["end_time"] = self.end_time
            results["duration_seconds"] = duration
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed in {duration:.2f} seconds"
            )
            
            # Save logs
            self.logger.save_logs()
            
            return results
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            results["success"] = False
            results["end_time"] = self.end_time
            results["error"] = str(e)
            
            logger.error(f"ETL failed: {str(e)}", exc_info=True)
            
            # Save error logs
            self.logger.save_logs()
            
            raise
    
    def display_summary(self) -> None:
        """Display execution summary"""
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)