"""
ETL orchestration module.
Coordinates extraction, transformation, and loading processes.
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import sys
from typing import Optional

from src.extract import create_extractor
from src.transform import create_transformer
from src.load import create_loader
from src.utils import setup_logger, load_config, generate_etl_run_id


class ETLOrchestrator:
    """Main orchestrator for the Sales ETL pipeline."""
    
    def __init__(self, config_path: str = 'config.yaml', test_mode: bool = False):
        """
        Initialize ETL orchestrator.
        
        Args:
            config_path: Path to configuration file
            test_mode: Run in test mode (no commits)
        """
        self.config = load_config(config_path)
        self.test_mode = test_mode
        self.etl_run_id = generate_etl_run_id()
        
        # Setup logging
        self.logger = setup_logger(
            log_level=self.config.get('logging', {}).get('level', 'INFO'),
            log_file=self.config.get('logging', {}).get('file')
        )
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        # Initialize ETL components
        self.extractor = create_extractor(self.spark, self.config, self.logger)
        self.transformer = create_transformer(self.config, self.logger, self.etl_run_id)
        self.loader = create_loader(self.spark, self.config, self.logger)
        
        # Statistics
        self.stats = {
            'etl_run_id': self.etl_run_id,
            'start_time': None,
            'end_time': None,
            'extract_stats': {},
            'transform_stats': {},
            'load_stats': {},
            'success': False
        }
        
        self.logger.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
        self.logger.info(f"Test mode: {test_mode}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder.appName(
            spark_config.get('app_name', 'SalesETL')
        )
        
        # Apply Spark configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(
            spark_config.get('log_level', 'WARN')
        )
        
        return spark
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None
    ) -> bool:
        """
        Execute complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source path
            target_path: Optional override for target path
            
        Returns:
            Success status
        """
        try:
            self.stats['start_time'] = datetime.now()
            self.logger.info("=" * 70)
            self.logger.info(f"Starting ETL process: {self.etl_run_id}")
            self.logger.info(f"Date range: {from_date} to {to_date}")
            self.logger.info("=" * 70)
            
            # Phase 1: Extract
            self.logger.info("\n=== EXTRACT Phase ===")
            raw_df, extract_stats = self.extractor.extract_sales_data(
                from_date, to_date, source_path
            )
            self.stats['extract_stats'] = extract_stats
            
            if extract_stats['records_extracted'] == 0:
                self.logger.warning("No records to process")
                self.stats['success'] = True
                return True
            
            # Phase 2: Transform
            self.logger.info("\n=== TRANSFORM Phase ===")
            analytics_df, transform_stats = self.transformer.transform_sales_data(raw_df)
            self.stats['transform_stats'] = transform_stats
            
            # Validate transformed data
            validation_passed, validation_errors = self.transformer.validate_transformed_data(
                analytics_df
            )
            if not validation_passed:
                raise ValueError(f"Data validation failed: {validation_errors}")
            
            # Phase 3: Load
            self.logger.info("\n=== LOAD Phase ===")
            load_success, load_stats = self.loader.load_analytics_data(
                analytics_df, target_path
            )
            self.stats['load_stats'] = load_stats
            
            if not load_success:
                raise RuntimeError("Data loading failed")
            
            # Update source status (if not in test mode)
            if not self.test_mode:
                trans_ids = [row.trans_id for row in raw_df.select('trans_id').collect()]
                self.loader.update_source_status(trans_ids)
            else:
                self.logger.info("Test mode - skipping source status update")
            
            # Finalize
            self.stats['end_time'] = datetime.now()
            self.stats['success'] = True
            
            self._display_summary()
            
            return True
            
        except Exception as e:
            self.stats['end_time'] = datetime.now()
            self.stats['success'] = False
            self.stats['error'] = str(e)
            
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            self._display_summary()
            
            return False
        
        finally:
            # Clean up
            raw_df.unpersist() if 'raw_df' in locals() else None
            analytics_df.unpersist() if 'analytics_df' in locals() else None
    
    def _display_summary(self):
        """Display ETL execution summary."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("ETL Process Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"ETL Run ID:       {self.stats['etl_run_id']}")
        self.logger.info(f"Start Time:       {self.stats['start_time']}")
        self.logger.info(f"End Time:         {self.stats['end_time']}")
        
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            self.logger.info(f"Duration:         {duration:.2f} seconds")
        
        self.logger.info(f"Status:           {'SUCCESS' if self.stats['success'] else 'FAILED'}")
        
        if self.stats.get('extract_stats'):
            self.logger.info(f"\nExtract Stats:")
            self.logger.info(f"  Records Extracted: {self.stats['extract_stats'].get('records_extracted', 0)}")
        
        if self.stats.get('transform_stats'):
            self.logger.info(f"\nTransform Stats:")
            self.logger.info(f"  Records Transformed: {self.stats['transform_stats'].get('records_transformed', 0)}")
            if 'category_breakdown' in self.stats['transform_stats']:
                self.logger.info(f"  Category Breakdown: {self.stats['transform_stats']['category_breakdown']}")
        
        if self.stats.get('load_stats'):
            self.logger.info(f"\nLoad Stats:")
            self.logger.info(f"  Records Loaded: {self.stats['load_stats'].get('records_loaded', 0)}")
            self.logger.info(f"  Target: {self.stats['load_stats'].get('target_path', 'N/A')}")
        
        if not self.stats['success'] and self.stats.get('error'):
            self.logger.error(f"\nError: {self.stats['error']}")
        
        self.logger.info("=" * 70)
    
    def get_stats(self) -> dict:
        """Get ETL execution statistics."""
        return self.stats
    
    def stop(self):
        """Stop Spark session and cleanup."""
        self.logger.info("Stopping Spark session")
        self.spark.stop()