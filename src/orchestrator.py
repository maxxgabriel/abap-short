"""
ETL orchestrator module that coordinates the entire pipeline.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging
import uuid

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.extractor = SalesDataExtractor(spark, config)
        self.transformer = SalesDataTransformer(spark, config)
        self.loader = SalesDataLoader(spark, config)
        
        # Track execution metrics
        self.metrics = {
            "etl_run_id": self.etl_run_id,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "status": "initialized"
        }
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            ETL run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}{unique_id}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None
    ) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional source data path
            target_path: Optional target data path
            
        Returns:
            True if ETL successful, False otherwise
        """
        self.metrics["start_time"] = datetime.now()
        self.logger.info(
            f"Starting ETL process {self.etl_run_id} "
            f"for date range {from_date} to {to_date}"
        )
        
        try:
            # === EXTRACT Phase ===
            self.logger.info("=" * 50)
            self.logger.info("EXTRACT Phase")
            self.logger.info("=" * 50)
            
            raw_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date,
                source_path=source_path
            )
            
            self.metrics["records_extracted"] = raw_df.count()
            
            if not self.extractor.validate_extracted_data(raw_df):
                self.logger.warning("Extracted data validation warnings present")
            
            # Cache for performance
            raw_df.cache()
            
            # === TRANSFORM Phase ===
            self.logger.info("=" * 50)
            self.logger.info("TRANSFORM Phase")
            self.logger.info("=" * 50)
            
            analytics_df = self.transformer.transform_data(
                raw_df=raw_df,
                etl_run_id=self.etl_run_id
            )
            
            self.metrics["records_transformed"] = analytics_df.count()
            
            # Validate and log statistics
            validation_stats = self.transformer.validate_transformed_data(
                analytics_df
            )
            self.metrics.update(validation_stats)
            
            # Cache transformed data
            analytics_df.cache()
            
            # === LOAD Phase ===
            self.logger.info("=" * 50)
            self.logger.info("LOAD Phase")
            self.logger.info("=" * 50)
            
            load_success = self.loader.load_data(
                analytics_df=analytics_df,
                target_path=target_path
            )
            
            if not load_success:
                raise Exception("Data load failed")
            
            self.metrics["records_loaded"] = analytics_df.count()
            
            # Complete successfully
            self.metrics["end_time"] = datetime.now()
            self.metrics["duration_seconds"] = (
                self.metrics["end_time"] - self.metrics["start_time"]
            ).total_seconds()
            self.metrics["status"] = "success"
            
            self.logger.info("=" * 50)
            self.logger.info("ETL Process Completed Successfully")
            self.logger.info("=" * 50)
            self._display_summary()
            
            # Cleanup
            raw_df.unpersist()
            analytics_df.unpersist()
            
            return True
            
        except Exception as e:
            self.metrics["end_time"] = datetime.now()
            self.metrics["status"] = "failed"
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            return False
    
    def _display_summary(self):
        """Display ETL execution summary."""
        self.logger.info("ETL Execution Summary:")
        self.logger.info(f"  Run ID: {self.metrics['etl_run_id']}")
        self.logger.info(f"  Status: {self.metrics['status']}")
        self.logger.info(
            f"  Start Time: {self.metrics['start_time'].strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.logger.info(
            f"  End Time: {self.metrics['end_time'].strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.logger.info(
            f"  Duration: {self.metrics['duration_seconds']:.2f} seconds"
        )
        self.logger.info(
            f"  Records Extracted: {self.metrics['records_extracted']}"
        )
        self.logger.info(
            f"  Records Transformed: {self.metrics['records_transformed']}"
        )
        self.logger.info(
            f"  Records Loaded: {self.metrics['records_loaded']}"
        )
        
        # Display category breakdown if available
        for key in ["category_high", "category_medium", "category_low"]:
            if key in self.metrics:
                self.logger.info(f"  {key.replace('_', ' ').title()}: {self.metrics[key]}")
    
    def get_metrics(self) -> Dict:
        """
        Get ETL execution metrics.
        
        Returns:
            Dictionary containing execution metrics
        """
        return self.metrics.copy()