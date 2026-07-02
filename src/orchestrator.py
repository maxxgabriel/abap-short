"""
ETL orchestration module for Sales ETL pipeline.
Coordinates the extract, transform, and load operations.
"""

from pyspark.sql import SparkSession
import logging
from datetime import datetime
from typing import Dict, Tuple
import uuid

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = self._setup_logger()
        
        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger)
        self.transformer = SalesTransformer(self.logger, config)
        self.loader = SalesLoader(self.logger, config)
        
        self.start_time = None
        self.end_time = None
        self.statistics = {
            "extracted": 0,
            "transformed": 0,
            "loaded": 0,
            "errors": 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """
        Set up logging for the ETL process.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            Unique ETL run ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}{unique_id}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        source_path: str = None,
        target_path: str = None,
        use_sample_data: bool = False
    ) -> Tuple[bool, Dict]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            source_path: Path to source data (optional if using sample data)
            target_path: Path to target storage (optional)
            use_sample_data: Whether to use sample data for testing
        
        Returns:
            Tuple of (success flag, statistics dictionary)
        """
        try:
            self.start_time = datetime.now()
            self.logger.info("=" * 70)
            self.logger.info(f"ETL Process Started - Run ID: {self.etl_run_id}")
            self.logger.info(f"Date Range: {from_date} to {to_date}")
            self.logger.info("=" * 70)
            
            # Step 1: Extract
            self.logger.info("PHASE 1: EXTRACT")
            self.logger.info("-" * 70)
            
            if use_sample_data:
                df_raw, extract_success = self.extractor.extract_sample_data()
            else:
                if not source_path:
                    source_path = self.config["paths"]["source"]
                df_raw, extract_success = self.extractor.extract_data(
                    source_path, from_date, to_date
                )
            
            if not extract_success:
                raise Exception("Extraction phase failed")
            
            self.statistics["extracted"] = df_raw.count()
            
            # Step 2: Transform
            self.logger.info("")
            self.logger.info("PHASE 2: TRANSFORM")
            self.logger.info("-" * 70)
            
            df_analytics, transform_success = self.transformer.transform_data(
                df_raw, self.etl_run_id
            )
            
            if not transform_success:
                raise Exception("Transformation phase failed")
            
            # Validate transformed data
            df_valid, invalid_count = self.transformer.validate_transformed_data(df_analytics)
            
            self.statistics["transformed"] = df_valid.count()
            self.statistics["errors"] = invalid_count
            
            # Step 3: Load
            self.logger.info("")
            self.logger.info("PHASE 3: LOAD")
            self.logger.info("-" * 70)
            
            if not target_path:
                target_path = self.config["paths"]["target"]
            
            loaded_count, load_success = self.loader.load_data(
                df_valid,
                target_path,
                target_format=self.config["load"]["format"],
                mode=self.config["load"]["mode"]
            )
            
            if not load_success:
                raise Exception("Load phase failed")
            
            self.statistics["loaded"] = loaded_count
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.info("")
            self.logger.info("=" * 70)
            self.logger.info("ETL Process Completed Successfully")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Records Extracted: {self.statistics['extracted']}")
            self.logger.info(f"Records Transformed: {self.statistics['transformed']}")
            self.logger.info(f"Records Loaded: {self.statistics['loaded']}")
            self.logger.info(f"Errors: {self.statistics['errors']}")
            self.logger.info("=" * 70)
            
            return True, self.statistics
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error("=" * 70)
            self.logger.error(f"ETL Process Failed: {str(e)}")
            self.logger.error("=" * 70)
            return False, self.statistics
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def get_statistics(self) -> Dict:
        """
        Get ETL execution statistics.
        
        Returns:
            Statistics dictionary
        """
        return self.statistics