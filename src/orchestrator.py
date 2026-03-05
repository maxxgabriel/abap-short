"""
ETL Orchestrator - Main coordination module for Sales ETL Pipeline.
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import yaml
from pathlib import Path
from typing import Optional

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the orchestrator.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.etl_run_id = self._generate_etl_run_id()
        self.spark = self._create_spark_session()
        self.start_time = None
        self.end_time = None
        
        # Initialize ETL components
        self.extractor = SalesDataExtractor(self.spark, self.config['extraction'])
        self.transformer = SalesDataTransformer(self.config['transformation'])
        self.loader = SalesDataLoader(self.config['loading'])
        
        logger.info(f"ETL Orchestrator initialized with run ID: {self.etl_run_id}")
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'SalesETL'))
        
        # Apply configuration settings
        for key, value in spark_config.get('conf', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(
            spark_config.get('log_level', 'WARN')
        )
        
        logger.info("Spark session created successfully")
        return spark
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run identifier."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run(self, from_date: str, to_date: str) -> dict:
        """
        Execute complete ETL pipeline.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with execution results and statistics
        """
        self.start_time = datetime.now()
        results = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat(),
            "success": False,
            "phases": {}
        }
        
        try:
            logger.info(f"=" * 70)
            logger.info(f"Starting ETL process: {self.etl_run_id}")
            logger.info(f"Date range: {from_date} to {to_date}")
            logger.info(f"=" * 70)
            
            # Phase 1: Extract
            logger.info("\n=== EXTRACT Phase ===")
            df_raw = self.extractor.extract(from_date, to_date)
            
            if df_raw is None or df_raw.count() == 0:
                raise ValueError("No data extracted from source")
            
            if not self.extractor.validate_data(df_raw):
                raise ValueError("Extracted data failed validation")
            
            extract_count = df_raw.count()
            results["phases"]["extract"] = {
                "success": True,
                "records": extract_count
            }
            logger.info(f"Extract phase completed: {extract_count} records")
            
            # Phase 2: Transform
            logger.info("\n=== TRANSFORM Phase ===")
            df_analytics = self.transformer.transform(df_raw, self.etl_run_id)
            
            transform_count = df_analytics.count()
            transform_summary = self.transformer.get_transformation_summary(df_analytics)
            
            results["phases"]["transform"] = {
                "success": True,
                "records": transform_count,
                "summary": transform_summary
            }
            logger.info(f"Transform phase completed: {transform_count} records")
            logger.info(f"Transformation summary: {transform_summary}")
            
            # Phase 3: Load
            logger.info("\n=== LOAD Phase ===")
            load_result = self.loader.load_with_validation_report(df_analytics)
            
            results["phases"]["load"] = load_result
            logger.info(f"Load phase completed: {load_result}")
            
            if not load_result.get("success"):
                raise ValueError("Load phase failed")
            
            # Mark as successful
            results["success"] = True
            
            self.end_time = datetime.now()
            results["end_time"] = self.end_time.isoformat()
            results["duration_seconds"] = (self.end_time - self.start_time).total_seconds()
            
            logger.info(f"\n{'=' * 70}")
            logger.info(f"ETL process completed successfully")
            logger.info(f"Duration: {results['duration_seconds']:.2f} seconds")
            logger.info(f"{'=' * 70}")
            
        except Exception as e:
            self.end_time = datetime.now()
            results["end_time"] = self.end_time.isoformat()
            results["error"] = str(e)
            results["duration_seconds"] = (self.end_time - self.start_time).total_seconds()
            
            logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            logger.error(f"Duration before failure: {results['duration_seconds']:.2f} seconds")
        
        return results
    
    def display_summary(self, results: dict):
        """
        Display execution summary.
        
        Args:
            results: Results dictionary from run()
        """
        print("\n" + "=" * 70)
        print("ETL EXECUTION SUMMARY")
        print("=" * 70)
        print(f"ETL Run ID:       {results['etl_run_id']}")
        print(f"Start Time:       {results['start_time']}")
        print(f"End Time:         {results['end_time']}")
        print(f"Duration:         {results['duration_seconds']:.2f} seconds")
        print(f"Status:           {'SUCCESS' if results['success'] else 'FAILED'}")
        
        if "phases" in results:
            print("\nPhase Details:")
            for phase_name, phase_data in results["phases"].items():
                print(f"\n  {phase_name.upper()}:")
                if isinstance(phase_data, dict):
                    for key, value in phase_data.items():
                        if key != "summary":
                            print(f"    {key}: {value}")
        
        if "error" in results:
            print(f"\nError: {results['error']}")
        
        print("=" * 70 + "\n")
    
    def stop(self):
        """Stop Spark session and cleanup resources."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")