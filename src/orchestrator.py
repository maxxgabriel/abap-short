"""
ETL Orchestrator Module
Coordinates the complete ETL pipeline
"""
from pyspark.sql import SparkSession
from typing import Dict, Optional
import logging
from datetime import datetime
import uuid

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import DeltaLakeLoader


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    Equivalent to ZCL_ETL_ORCHESTRATOR in ABAP.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Generate ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.extractor = SalesDataExtractor(spark, config)
        self.transformer = SalesDataTransformer(spark, config)
        self.loader = DeltaLakeLoader(spark, config)
        
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        self.logger.info(f"ETL process initialized with run ID: {self.etl_run_id}")
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique ETL run identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str
    ) -> Dict[str, any]:
        """
        Execute complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary with ETL execution statistics
        """
        self.logger.info(f"=== Starting ETL Process ===")
        self.logger.info(f"Run ID: {self.etl_run_id}")
        self.logger.info(f"Date Range: {from_date} to {to_date}")
        
        self.start_time = datetime.now()
        
        etl_stats = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat(),
            "success": False,
            "extract": {},
            "transform": {},
            "load": {}
        }
        
        try:
            # Phase 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(from_date, to_date)
            extract_count = raw_df.count()
            
            etl_stats["extract"] = {
                "records_extracted": extract_count,
                "success": True
            }
            
            if extract_count == 0:
                self.logger.warning("No records extracted, terminating ETL")
                etl_stats["message"] = "No records to process"
                return etl_stats
            
            # Phase 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(raw_df, self.etl_run_id)
            transform_count = analytics_df.count()
            
            etl_stats["transform"] = {
                "records_transformed": transform_count,
                "success": True
            }
            
            # Phase 3: Load
            self.logger.info("=== LOAD Phase ===")
            load_stats = self.loader.load_data(analytics_df, self.etl_run_id)
            
            etl_stats["load"] = load_stats
            
            # Calculate final statistics
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            etl_stats["end_time"] = self.end_time.isoformat()
            etl_stats["duration_seconds"] = duration
            etl_stats["success"] = load_stats["success"]
            
            self.logger.info("=== ETL Process Complete ===")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Records Processed: {extract_count}")
            self.logger.info(f"Records Loaded: {load_stats['loaded_records']}")
            
            return etl_stats
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"ETL process failed: {str(e)}")
            
            etl_stats["success"] = False
            etl_stats["error"] = str(e)
            etl_stats["end_time"] = self.end_time.isoformat() if self.end_time else None
            
            raise
    
    def get_etl_run_id(self) -> str:
        """
        Get current ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def display_summary(self):
        """
        Display ETL execution summary.
        """
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)