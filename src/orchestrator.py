"""
PySpark ETL Orchestrator Module
Main orchestrator coordinating extract, transform, and load operations.
"""

from datetime import datetime, date
from typing import Optional
from pyspark.sql import SparkSession
from src.logger import ETLLogger
from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader


class ETLOrchestrator:
    """
    Main ETL orchestrator coordinating all ETL phases.
    Uses dependency injection for all components.
    """
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize orchestrator with Spark session and configuration.
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id, spark)
        
        # Initialize components with dependency injection
        self.extractor = SalesDataExtractor(spark, self.logger)
        self.transformer = SalesDataTransformer(self.logger, config)
        self.loader = SalesDataLoader(self.logger, config)
        
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def run_etl(
        self, 
        from_date: date, 
        to_date: date,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None
    ) -> bool:
        """
        Execute complete ETL process.
        
        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            source_path: Optional source data path
            target_path: Optional target data path
            
        Returns:
            True if ETL successful, False otherwise
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            print("\n" + "="*70)
            print("=== EXTRACT Phase ===")
            print("="*70)
            
            # Extract
            raw_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date,
                source_path=source_path
            )
            
            print("\n" + "="*70)
            print("=== TRANSFORM Phase ===")
            print("="*70)
            
            # Transform
            analytics_df = self.transformer.transform_data(raw_df)
            
            print("\n" + "="*70)
            print("=== LOAD Phase ===")
            print("="*70)
            
            # Load
            load_success = self.loader.load_data(
                analytics_df=analytics_df,
                target_path=target_path
            )
            
            if not load_success:
                raise RuntimeError("Load phase failed")
            
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            error_msg = f"ETL process failed: {str(e)}"
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=error_msg
            )
            
            return False
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique ETL run identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display ETL execution summary."""
        print("\n" + "="*70)
        print("ETL Process Summary")
        print("="*70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        
        if self.start_time:
            print(f"Start Time:    {self.start_time.isoformat()}")
        
        if self.end_time:
            print(f"End Time:      {self.end_time.isoformat()}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("="*70 + "\n")