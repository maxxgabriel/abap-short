"""
Sales ETL - Orchestrator Module
Main orchestrator that coordinates the ETL process.
"""

from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Tuple

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.utils.logger import ETLLogger
from src.utils.config import load_config
from src.utils.exceptions import ETLError


class ETLOrchestrator:
    """Main ETL orchestrator for sales data processing."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the orchestrator.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.spark = self._create_spark_session()
        self.logger = ETLLogger(self.spark, self.config)
        
        # Initialize ETL components
        self.extractor = SalesExtractor(self.spark, self.logger, self.config)
        self.transformer = SalesTransformer(self.spark, self.logger, self.config)
        self.loader = SalesLoader(self.spark, self.logger, self.config)
        
        self.start_time = None
        self.end_time = None
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession."""
        spark_config = self.config.get("spark", {})
        
        builder = SparkSession.builder.appName(
            spark_config.get("app_name", "SalesETL")
        )
        
        # Add configurations
        for key, value in spark_config.get("configs", {}).items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        source_type: str = None,
        target_type: str = None
    ) -> Tuple[bool, dict]:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_type: Optional source type override
            target_type: Optional target type override
            
        Returns:
            Tuple of (success, statistics)
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="I",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Use configured types if not provided
            if source_type is None:
                source_type = self.config.get("source", {}).get("type", "parquet")
            if target_type is None:
                target_type = self.config.get("target", {}).get("type", "parquet")
            
            # Step 1: Extract
            print(f"\n{'='*60}")
            print("=== EXTRACT Phase ===")
            print(f"{'='*60}")
            
            raw_df = self.extractor.extract_from_source(
                from_date=from_date,
                to_date=to_date,
                source_type=source_type
            )
            
            extract_count = raw_df.count()
            
            if extract_count == 0:
                self.logger.log_message(
                    step="EXTRACT",
                    status="W",
                    message="No data found for the specified date range"
                )
                return False, self._get_statistics(0, 0, 0)
            
            # Step 2: Transform
            print(f"\n{'='*60}")
            print("=== TRANSFORM Phase ===")
            print(f"{'='*60}")
            
            analytics_df, transform_success, transform_errors = \
                self.transformer.transform_data(raw_df)
            
            # Step 3: Load
            print(f"\n{'='*60}")
            print("=== LOAD Phase ===")
            print(f"{'='*60}")
            
            load_success, load_count, load_errors = \
                self.loader.load_data(analytics_df, target_type=target_type)
            
            if not load_success:
                raise ETLError("Load phase failed")
            
            # Complete
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            statistics = self._get_statistics(
                extract_count, 
                transform_success, 
                load_count
            )
            
            return True, statistics
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            return False, self._get_statistics(0, 0, 0)
    
    def _get_statistics(
        self, 
        extracted: int, 
        transformed: int, 
        loaded: int
    ) -> dict:
        """
        Get ETL statistics.
        
        Args:
            extracted: Number of records extracted
            transformed: Number of records transformed
            loaded: Number of records loaded
            
        Returns:
            Dictionary with statistics
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            "etl_run_id": self.logger.etl_run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "records_extracted": extracted,
            "records_transformed": transformed,
            "records_loaded": loaded,
            "success_rate": (loaded / extracted * 100) if extracted > 0 else 0
        }
    
    def display_summary(self, statistics: dict):
        """
        Display ETL summary.
        
        Args:
            statistics: Statistics dictionary
        """
        print(f"\n{'='*60}")
        print("ETL Process Summary")
        print(f"{'='*60}")
        print(f"ETL Run ID:       {statistics['etl_run_id']}")
        print(f"Start Time:       {statistics['start_time']}")
        print(f"End Time:         {statistics['end_time']}")
        if statistics['duration_seconds']:
            print(f"Duration:         {statistics['duration_seconds']:.2f} seconds")
        print(f"Extracted:        {statistics['records_extracted']}")
        print(f"Transformed:      {statistics['records_transformed']}")
        print(f"Loaded:           {statistics['records_loaded']}")
        print(f"Success Rate:     {statistics['success_rate']:.2f}%")
        print(f"{'='*60}\n")
    
    def cleanup(self):
        """Cleanup resources."""
        if self.spark:
            self.spark.stop()