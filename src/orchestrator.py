"""
ETL orchestration module.
Coordinates extract, transform, and load operations.
"""
from pyspark.sql import SparkSession
import logging
from datetime import datetime
import uuid
from typing import Optional

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Main ETL orchestrator that coordinates the complete pipeline."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize orchestrator with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.extractor = SalesExtractor(spark, config)
        self.transformer = SalesTransformer(config)
        self.loader = SalesLoader(spark, config)
        
        # Track execution metrics
        self.metrics = {
            "etl_run_id": self.etl_run_id,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "status": "INITIALIZED"
        }
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            Unique ETL run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}{unique_id}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        use_sample_data: bool = False
    ) -> dict:
        """
        Execute complete ETL pipeline.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            use_sample_data: If True, use sample data instead of actual extraction
            
        Returns:
            Dictionary containing execution metrics and status
        """
        self.metrics["start_time"] = datetime.now()
        
        self.logger.info("=" * 70)
        self.logger.info(f"ETL Process Started - Run ID: {self.etl_run_id}")
        self.logger.info(f"Date Range: {from_date} to {to_date}")
        self.logger.info("=" * 70)
        
        try:
            # Phase 1: EXTRACT
            self.logger.info("\n=== EXTRACT Phase ===")
            self.metrics["status"] = "EXTRACTING"
            
            if use_sample_data:
                raw_df = self.extractor.extract_sample_data()
            else:
                raw_df = self.extractor.extract_data(from_date, to_date)
            
            self.metrics["records_extracted"] = raw_df.count()
            self.logger.info(f"Extraction completed: {self.metrics['records_extracted']} records")
            
            if self.metrics["records_extracted"] == 0:
                self.logger.warning("No records to process")
                self.metrics["status"] = "NO_DATA"
                return self._finalize_metrics()
            
            # Phase 2: TRANSFORM
            self.logger.info("\n=== TRANSFORM Phase ===")
            self.metrics["status"] = "TRANSFORMING"
            
            analytics_df = self.transformer.transform_data(raw_df, self.etl_run_id)
            self.metrics["records_transformed"] = analytics_df.count()
            self.logger.info(f"Transformation completed: {self.metrics['records_transformed']} records")
            
            # Validate transformed data
            if not self.transformer.validate_transformed_data(analytics_df):
                raise ValueError("Transformed data validation failed")
            
            # Phase 3: LOAD
            self.logger.info("\n=== LOAD Phase ===")
            self.metrics["status"] = "LOADING"
            
            load_result = self.loader.load_data(analytics_df)
            
            if not load_result["success"]:
                raise ValueError(f"Load failed: {load_result.get('error', 'Unknown error')}")
            
            self.metrics["records_loaded"] = load_result["records_loaded"]
            self.logger.info(f"Load completed: {self.metrics['records_loaded']} records")
            
            # Validate load
            if not self.loader.validate_load(
                self.config['target']['table_name'],
                self.etl_run_id
            ):
                raise ValueError("Load validation failed")
            
            # Update source table status (optional)
            if self.config.get('update_source_status', True):
                processed_ids = [row.trans_id for row in raw_df.select("trans_id").collect()]
                self.loader.update_source_status(processed_ids)
            
            self.metrics["status"] = "SUCCESS"
            self.logger.info("\n*** ETL Process Completed Successfully ***")
            
        except Exception as e:
            self.metrics["status"] = "FAILED"
            self.logger.error(f"\n*** ETL Process Failed ***")
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            self.metrics["error"] = str(e)
        
        finally:
            return self._finalize_metrics()
    
    def _finalize_metrics(self) -> dict:
        """
        Finalize execution metrics.
        
        Returns:
            Complete metrics dictionary
        """
        self.metrics["end_time"] = datetime.now()
        
        if self.metrics["start_time"] and self.metrics["end_time"]:
            duration = self.metrics["end_time"] - self.metrics["start_time"]
            self.metrics["duration_seconds"] = duration.total_seconds()
        
        self._display_summary()
        return self.metrics
    
    def _display_summary(self) -> None:
        """Display execution summary."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("ETL Process Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"ETL Run ID:       {self.metrics['etl_run_id']}")
        self.logger.info(f"Status:           {self.metrics['status']}")
        self.logger.info(f"Start Time:       {self.metrics['start_time']}")
        self.logger.info(f"End Time:         {self.metrics['end_time']}")
        self.logger.info(f"Duration:         {self.metrics.get('duration_seconds', 0):.2f} seconds")
        self.logger.info(f"Records Extracted: {self.metrics['records_extracted']}")
        self.logger.info(f"Records Transformed: {self.metrics['records_transformed']}")
        self.logger.info(f"Records Loaded:    {self.metrics['records_loaded']}")
        
        if "error" in self.metrics:
            self.logger.info(f"Error:            {self.metrics['error']}")
        
        self.logger.info("=" * 70)
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_metrics(self) -> dict:
        """
        Get execution metrics.
        
        Returns:
            Dictionary containing all execution metrics
        """
        return self.metrics.copy()