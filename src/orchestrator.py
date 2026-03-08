"""
ETL Orchestrator
Replaces ZCL_ETL_ORCHESTRATOR class
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.exceptions import ETLError


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process
    Replaces ZCL_ETL_ORCHESTRATOR
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        test_mode: bool = False
    ):
        """Initialize orchestrator with Spark session and configuration"""
        self.spark = spark
        self.config = config
        self.test_mode = test_mode
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = DataExtractor(spark, config, self.logger)
        self.transformer = DataTransformer(spark, config, self.logger)
        self.loader = DataLoader(spark, config, self.logger)
        
        # Initialize timing
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        prefix = self.config.get('id_prefixes', {}).get('etl_run', 'ETL')
        return f"{prefix}{timestamp}"
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL process
        Replaces run_etl method from ABAP
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            print("=== EXTRACT Phase ===")
            raw_data = self.extractor.extract_data(from_date, to_date)
            
            if raw_data is None or raw_data.count() == 0:
                raise ETLError("Extraction failed or returned no data")
            
            # Step 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_data = self.transformer.transform_data(raw_data)
            
            if analytics_data is None or analytics_data.count() == 0:
                raise ETLError("Transformation failed or returned no data")
            
            # Step 3: Load
            print("\n=== LOAD Phase ===")
            if not self.test_mode:
                success = self.loader.load_data(analytics_data)
                if not success:
                    raise ETLError("Load failed")
            else:
                print("Test mode - skipping actual data load")
                # Show sample data in test mode
                print("\nSample transformed data (first 5 rows):")
                analytics_data.show(5, truncate=False)
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f"ETL process failed: {str(e)}"
            )
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display execution summary"""
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