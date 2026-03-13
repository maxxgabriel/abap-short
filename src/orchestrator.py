"""
Orchestrator module for Sales ETL Pipeline
Coordinates the complete ETL process
"""
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import yaml
from pathlib import Path
from typing import Optional, Dict

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader


class ETLOrchestrator:
    """Main orchestrator for the ETL pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the ETL orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.etl_run_id = self._generate_run_id()
        self.logger = self._setup_logging()
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.extractor = SalesExtractor(self.spark, self.config, self.logger)
        self.transformer = SalesTransformer(self.spark, self.config, self.logger, self.etl_run_id)
        self.loader = SalesLoader(self.spark, self.config, self.logger)
        
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict = {}
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _generate_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format
        )
        
        logger = logging.getLogger('SalesETL')
        
        # Add file handler if configured
        if 'log_file' in log_config:
            fh = logging.FileHandler(log_config['log_file'])
            fh.setFormatter(logging.Formatter(log_format))
            logger.addHandler(fh)
        
        return logger
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder.appName(f"SalesETL-{self.etl_run_id}")
        
        # Apply Spark configurations
        for key, value in spark_config.items():
            if key not in ['app_name']:
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        self.logger.info(f"Spark session created: {spark.version}")
        return spark
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        test_mode: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            test_mode: If True, don't commit data
            
        Returns:
            bool: True if ETL completed successfully
        """
        try:
            self.start_time = datetime.now()
            self.logger.info("=" * 70)
            self.logger.info(f"ETL Process Started - Run ID: {self.etl_run_id}")
            self.logger.info(f"Date Range: {from_date} to {to_date}")
            self.logger.info(f"Test Mode: {test_mode}")
            self.logger.info("=" * 70)
            
            # Step 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(from_date, to_date)
            
            if not self.extractor.validate_extracted_data(raw_df):
                raise ValueError("Extracted data validation failed")
            
            extract_count = raw_df.count()
            self.statistics['extracted'] = extract_count
            
            # Step 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df)
            
            if not self.transformer.validate_transformed_data(analytics_df):
                raise ValueError("Transformed data validation failed")
            
            transform_count = analytics_df.count()
            self.statistics['transformed'] = transform_count
            
            # Step 3: Load
            self.logger.info("=== LOAD Phase ===")
            
            if test_mode:
                self.logger.info("Test mode - skipping actual load")
                load_success = True
                self.statistics['loaded'] = transform_count
            else:
                load_success = self.loader.load_data(analytics_df)
                if load_success:
                    self.statistics['loaded'] = transform_count
            
            if not load_success:
                raise ValueError("Load failed")
            
            # Generate summary report
            summary = self.loader.create_summary_report(analytics_df)
            self.statistics['summary'] = summary
            
            self.end_time = datetime.now()
            self._display_summary()
            
            self.logger.info("ETL Process Completed Successfully")
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"ETL Process Failed: {str(e)}")
            self.logger.exception("Full traceback:")
            return False
        
        finally:
            if self.config.get('spark', {}).get('stop_session', True):
                self.spark.stop()
    
    def _display_summary(self):
        """Display ETL execution summary"""
        duration = (self.end_time - self.start_time).total_seconds()
        
        self.logger.info("=" * 70)
        self.logger.info("ETL Process Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"ETL Run ID:        {self.etl_run_id}")
        self.logger.info(f"Start Time:        {self.start_time}")
        self.logger.info(f"End Time:          {self.end_time}")
        self.logger.info(f"Duration:          {duration:.2f} seconds")
        self.logger.info(f"Records Extracted: {self.statistics.get('extracted', 0)}")
        self.logger.info(f"Records Transformed: {self.statistics.get('transformed', 0)}")
        self.logger.info(f"Records Loaded:    {self.statistics.get('loaded', 0)}")
        
        if 'summary' in self.statistics:
            summary = self.statistics['summary']
            self.logger.info(f"Total Gross Amount: {summary.get('total_gross_amount', 0):.2f}")
            self.logger.info(f"Total Net Amount:   {summary.get('total_net_amount', 0):.2f}")
        
        self.logger.info("=" * 70)
    
    def get_run_id(self) -> str:
        """Get the current ETL run ID"""
        return self.etl_run_id