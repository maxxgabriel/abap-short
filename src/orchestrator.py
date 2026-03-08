"""
ETL orchestrator module.
Coordinates the complete ETL pipeline execution.
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLOrchestrator:
    """Main orchestrator for the ETL pipeline."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the ETL orchestrator.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = ETLConfig(config_path)
        self.log = logging.getLogger(__name__)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Generate ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(self.spark, self.etl_run_id, self.config)
        
        # Initialize ETL components
        self.extractor = DataExtractor(self.spark, self.logger, self.config)
        self.transformer = DataTransformer(self.spark, self.logger, self.config)
        self.loader = DataLoader(self.spark, self.logger, self.config)
        
        # Track execution metrics
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.metrics: Dict[str, Any] = {}
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.
        
        Returns:
            Configured SparkSession
        """
        app_name = self.config.get("spark.app_name", "SalesETL")
        
        builder = SparkSession.builder.appName(app_name)
        
        # Apply Spark configurations
        spark_configs = self.config.get("spark.config", {})
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        log_level = self.config.get("spark.log_level", "WARN")
        spark.sparkContext.setLogLevel(log_level)
        
        self.log.info(f"Spark session created: {app_name}")
        return spark
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        test_mode: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            test_mode: If True, don't commit changes
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            self.log.info("=" * 60)
            self.log.info("EXTRACT Phase")
            self.log.info("=" * 60)
            
            # Step 1: Extract
            raw_df = self.extractor.extract_data(from_date, to_date)
            if raw_df is None:
                raise Exception("Extraction failed")
            
            self.metrics["extracted_records"] = raw_df.count()
            
            # Cache for reuse
            raw_df.cache()
            
            self.log.info("=" * 60)
            self.log.info("TRANSFORM Phase")
            self.log.info("=" * 60)
            
            # Step 2: Transform
            analytics_df = self.transformer.transform_data(raw_df)
            if analytics_df is None:
                raise Exception("Transformation failed")
            
            # Validate transformed data
            validated_df = self.transformer.validate_transformed_data(analytics_df)
            self.metrics["transformed_records"] = validated_df.count()
            
            # Cache transformed data
            validated_df.cache()
            
            self.log.info("=" * 60)
            self.log.info("LOAD Phase")
            self.log.info("=" * 60)
            
            # Step 3: Load
            write_mode = "append" if not test_mode else "overwrite"
            success = self.loader.load_data(validated_df, mode=write_mode)
            
            if not success:
                raise Exception("Load failed")
            
            self.metrics["loaded_records"] = validated_df.count()
            
            # Unpersist cached DataFrames
            raw_df.unpersist()
            validated_df.unpersist()
            
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            # Display summary
            self.display_summary()
            
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
        
        finally:
            # Save logs
            self.logger.save_logs()
    
    def display_summary(self) -> None:
        """Display ETL execution summary."""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        print(f"Duration:      {duration:.2f} seconds")
        print("-" * 60)
        print(f"Extracted:     {self.metrics.get('extracted_records', 0)} records")
        print(f"Transformed:   {self.metrics.get('transformed_records', 0)} records")
        print(f"Loaded:        {self.metrics.get('loaded_records', 0)} records")
        print("=" * 60 + "\n")
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def stop(self) -> None:
        """Stop the Spark session and cleanup resources."""
        if self.spark:
            self.spark.stop()
            self.log.info("Spark session stopped")