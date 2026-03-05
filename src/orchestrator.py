"""
ETL orchestration module.
Coordinates the execution of extract, transform, and load operations.
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Tuple

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(spark, self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = DataExtractor(spark, self.logger)
        self.transformer = DataTransformer(self.logger, config)
        self.loader = DataLoader(self.logger, config)
        
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(
        self, 
        from_date: str, 
        to_date: str,
        use_sample_data: bool = False
    ) -> bool:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            use_sample_data: Use sample data instead of reading from source
            
        Returns:
            Success flag
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
            
            # Step 1: Extract
            if use_sample_data:
                raw_df = self.extractor.create_sample_data()
                extract_success = True
            else:
                raw_df, extract_success = self.extractor.extract_data(
                    from_date, 
                    to_date,
                    self.config['tables']['source_table']
                )
            
            if not extract_success:
                raise RuntimeError("Extraction failed")
            
            print("\n" + "="*70)
            print("=== TRANSFORM Phase ===")
            print("="*70)
            
            # Step 2: Transform
            analytics_df, transform_success = self.transformer.transform_data(
                raw_df,
                self.etl_run_id
            )
            
            if not transform_success:
                raise RuntimeError("Transformation failed")
            
            print("\n" + "="*70)
            print("=== LOAD Phase ===")
            print("="*70)
            
            # Step 3: Load
            load_success = self.loader.load_data(
                analytics_df,
                self.config['tables']['target_table']
            )
            
            if not load_success:
                raise RuntimeError("Load failed")
            
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            # Persist logs
            self.logger.persist_logs(self.config['tables']['log_table'])
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            # Attempt to persist logs even on failure
            try:
                self.logger.persist_logs(self.config['tables']['log_table'])
            except:
                pass
            
            return False
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display ETL execution summary."""
        print("\n" + "="*70)
        print("ETL Process Summary")
        print("="*70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time.isoformat() if self.start_time else 'N/A'}")
        print(f"End Time:      {self.end_time.isoformat() if self.end_time else 'N/A'}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("="*70)
        
        # Display log summary
        log_entries = self.logger.get_log_entries()
        status_counts = {}
        for entry in log_entries:
            status = entry['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print("\nLog Summary:")
        for status, count in sorted(status_counts.items()):
            status_name = {
                'S': 'Success',
                'E': 'Error',
                'W': 'Warning',
                'I': 'Info'
            }.get(status, 'Unknown')
            print(f"  {status_name}: {count}")
        print("="*70 + "\n")
    
    def _generate_etl_run_id(self) -> str:
        """Generate a unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"