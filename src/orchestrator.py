"""
ETL Orchestration Pipeline
Coordinates Extract, Transform, Load phases with retry logic and error handling
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
from typing import Dict, Any, Optional, Tuple
import yaml
import uuid

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractionError, TransformationError, LoadError


class ETLOrchestrator:
    """Main ETL orchestrator coordinating the entire pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize ETL orchestrator with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.etl_run_id = self._generate_etl_run_id()
        self.spark = self._create_spark_session()
        self.logger = ETLLogger(self.spark, self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = SalesExtractor(self.spark, self.logger, self.config)
        self.transformer = SalesTransformer(self.spark, self.logger, self.config)
        self.loader = SalesLoader(self.spark, self.logger, self.config)
        
        # Tracking variables
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict[str, Any] = {
            "total_extracted": 0,
            "total_transformed": 0,
            "total_loaded": 0,
            "errors_count": 0,
            "warnings_count": 0
        }
        
        logging.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logging.error(f"Failed to load config from {config_path}: {str(e)}")
            # Return default config
            return self._default_config()
    
    def _default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            "spark": {
                "app_name": "SalesETLPipeline",
                "master": "local[*]"
            },
            "processing": {
                "batch_size": 1000,
                "retry_attempts": 3,
                "retry_delay_seconds": 5
            },
            "business_rules": {
                "discount_qty_tier1": 10,
                "discount_qty_tier2": 15,
                "discount_rate_tier1": 0.05,
                "discount_rate_tier2": 0.10,
                "tax_rate": 0.08,
                "cost_ratio": 0.60,
                "category_high_threshold": 2000.00,
                "category_medium_threshold": 500.00
            }
        }
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config.get("spark", {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get("app_name", "SalesETLPipeline")) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200")
        
        # Add master if specified
        if "master" in spark_config:
            builder = builder.master(spark_config["master"])
        
        return builder.getOrCreate()
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run identifier"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL_{timestamp}_{unique_id}"
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        test_mode: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute complete ETL pipeline with orchestration
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            test_mode: If True, don't commit final results
            
        Returns:
            Tuple of (success, statistics)
        """
        self.start_time = datetime.now()
        success = False
        
        try:
            self.logger.log_message(
                step="INIT",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Phase 1: Extract
            logging.info("=" * 60)
            logging.info("EXTRACT Phase")
            logging.info("=" * 60)
            
            raw_df = self._execute_with_retry(
                self.extractor.extract_data,
                "EXTRACT",
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_df is None or raw_df.count() == 0:
                raise ExtractionError("No data extracted from source")
            
            self.statistics["total_extracted"] = raw_df.count()
            
            # Phase 2: Transform
            logging.info("=" * 60)
            logging.info("TRANSFORM Phase")
            logging.info("=" * 60)
            
            analytics_df = self._execute_with_retry(
                self.transformer.transform_data,
                "TRANSFORM",
                raw_df=raw_df,
                etl_run_id=self.etl_run_id
            )
            
            if analytics_df is None or analytics_df.count() == 0:
                raise TransformationError("Transformation produced no results")
            
            self.statistics["total_transformed"] = analytics_df.count()
            
            # Phase 3: Load
            logging.info("=" * 60)
            logging.info("LOAD Phase")
            logging.info("=" * 60)
            
            load_success, load_stats = self._execute_with_retry(
                self.loader.load_data,
                "LOAD",
                analytics_df=analytics_df,
                test_mode=test_mode
            )
            
            if not load_success:
                raise LoadError("Data load failed")
            
            self.statistics["total_loaded"] = load_stats.get("loaded", 0)
            self.statistics["errors_count"] = load_stats.get("errors", 0)
            
            # Mark as complete
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                records_processed=self.statistics["total_extracted"],
                records_success=self.statistics["total_loaded"],
                records_error=self.statistics["errors_count"],
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            success = True
            
        except ExtractionError as e:
            self._handle_error("EXTRACT", str(e))
        except TransformationError as e:
            self._handle_error("TRANSFORM", str(e))
        except LoadError as e:
            self._handle_error("LOAD", str(e))
        except Exception as e:
            self._handle_error("ERROR", f"Unexpected error: {str(e)}")
        finally:
            self.end_time = datetime.now() if self.end_time is None else self.end_time
            self._finalize_statistics()
        
        return success, self.statistics
    
    def _execute_with_retry(
        self, 
        func, 
        step_name: str,
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic
        
        Args:
            func: Function to execute
            step_name: Name of the processing step
            **kwargs: Arguments to pass to function
            
        Returns:
            Function result
        """
        retry_config = self.config.get("processing", {})
        max_attempts = retry_config.get("retry_attempts", 3)
        retry_delay = retry_config.get("retry_delay_seconds", 5)
        
        last_exception = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.log_message(
                    step=step_name,
                    status="I",
                    message=f"Attempt {attempt}/{max_attempts}"
                )
                
                result = func(**kwargs)
                
                if attempt > 1:
                    self.logger.log_message(
                        step=step_name,
                        status="W",
                        message=f"Succeeded on retry attempt {attempt}"
                    )
                
                return result
                
            except Exception as e:
                last_exception = e
                
                if attempt < max_attempts:
                    self.logger.log_message(
                        step=step_name,
                        status="W",
                        message=f"Attempt {attempt} failed: {str(e)}. Retrying in {retry_delay}s..."
                    )
                    import time
                    time.sleep(retry_delay)
                else:
                    self.logger.log_message(
                        step=step_name,
                        status="E",
                        message=f"All {max_attempts} attempts failed"
                    )
        
        # If we get here, all retries failed
        raise last_exception
    
    def _handle_error(self, step: str, error_message: str):
        """Handle errors during ETL process"""
        self.end_time = datetime.now()
        
        self.logger.log_message(
            step=step,
            status="E",
            message=f"ETL process failed: {error_message}"
        )
        
        logging.error(f"ETL Error in {step}: {error_message}")
    
    def _finalize_statistics(self):
        """Calculate final statistics"""
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            self.statistics["duration_seconds"] = duration
            self.statistics["start_time"] = self.start_time.isoformat()
            self.statistics["end_time"] = self.end_time.isoformat()
    
    def display_summary(self) -> str:
        """
        Generate and return summary report
        
        Returns:
            Formatted summary string
        """
        summary_lines = [
            "=" * 70,
            "ETL Process Summary",
            "=" * 70,
            f"ETL Run ID:       {self.etl_run_id}",
            f"Start Time:       {self.statistics.get('start_time', 'N/A')}",
            f"End Time:         {self.statistics.get('end_time', 'N/A')}",
            f"Duration:         {self.statistics.get('duration_seconds', 0):.2f} seconds",
            "",
            "Processing Statistics:",
            f"  - Extracted:    {self.statistics.get('total_extracted', 0):,}",
            f"  - Transformed:  {self.statistics.get('total_transformed', 0):,}",
            f"  - Loaded:       {self.statistics.get('total_loaded', 0):,}",
            f"  - Errors:       {self.statistics.get('errors_count', 0):,}",
            f"  - Warnings:     {self.statistics.get('warnings_count', 0):,}",
            "=" * 70
        ]
        
        summary = "\n".join(summary_lines)
        logging.info(summary)
        return summary
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run identifier"""
        return self.etl_run_id
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.spark:
                self.spark.stop()
                logging.info("Spark session stopped")
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    orchestrator = ETLOrchestrator("config.yaml")
    
    try:
        success, stats = orchestrator.run_etl(
            from_date="2024-01-01",
            to_date="2024-01-31",
            test_mode=True
        )
        
        orchestrator.display_summary()
        
        if success:
            print("\n✓ ETL Process Completed Successfully")
        else:
            print("\n✗ ETL Process Failed")
            
    finally:
        orchestrator.cleanup()