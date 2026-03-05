"""
ETL orchestrator that coordinates the entire pipeline.
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Dict, Any

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.monitoring import ETLMonitor


class ETLOrchestrator:
    """Orchestrates the ETL pipeline execution."""
    
    def __init__(self, config: dict):
        """
        Initialize the orchestrator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = ETLLogger()
        self.monitor = ETLMonitor(self.logger)
        
        # Initialize Spark
        self.spark = self._initialize_spark()
        
        # Initialize ETL components
        self.extractor = SalesExtractor(self.spark, self.logger)
        self.transformer = SalesTransformer(self.spark, self.logger, config)
        self.loader = SalesLoader(self.spark, self.logger, config)
        
        self.start_time = None
        self.end_time = None
        
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with configuration."""
        builder = SparkSession.builder.appName(self.config["spark"]["app_name"])
        
        # Apply Spark configurations
        for key, value in self.config["spark"]["config"].items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        source_path: str,
        target_path: str
    ) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Source data path
            target_path: Target data path
            
        Returns:
            Dictionary with execution results
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="INIT",
                status="S",
                message=f"ETL process initialized with run ID: {self.logger.etl_run_id}"
            )
            
            # Step 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_df, extract_success = self.extractor.extract_data(
                from_date, to_date, source_path
            )
            
            if not extract_success:
                raise Exception("Extraction failed")
            
            self.monitor.record_metric("extract_count", raw_df.count())
            
            # Step 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_df, transform_success = self.transformer.transform_data(raw_df)
            
            if not transform_success:
                raise Exception("Transformation failed")
            
            self.monitor.record_metric("transform_count", analytics_df.count())
            
            # Step 3: Load
            logging.info("=== LOAD Phase ===")
            success_count, error_count, load_success = self.loader.load_data(
                analytics_df, target_path
            )
            
            if not load_success:
                raise Exception("Load failed")
            
            self.monitor.record_metric("load_success", success_count)
            self.monitor.record_metric("load_error", error_count)
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully in {duration:.2f} seconds"
            )
            
            # Generate monitoring report
            report = self.monitor.generate_report(self.start_time, self.end_time)
            
            return {
                "success": True,
                "etl_run_id": self.logger.etl_run_id,
                "duration_seconds": duration,
                "metrics": report,
                "logs": self.logger.get_logs()
            }
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            logging.error(f"ETL process failed: {str(e)}", exc_info=True)
            
            return {
                "success": False,
                "etl_run_id": self.logger.etl_run_id,
                "error": str(e),
                "logs": self.logger.get_logs()
            }
        finally:
            # Cleanup
            if hasattr(self, 'spark'):
                self.spark.catalog.clearCache()
    
    def get_etl_run_id(self) -> str:
        """Return the ETL run ID."""
        return self.logger.etl_run_id
    
    def display_summary(self) -> None:
        """Display execution summary."""
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:    {self.logger.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 70)