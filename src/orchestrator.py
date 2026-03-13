"""
ETL Orchestrator Module
Coordinates the execution flow across all ETL phases with run management.
"""

from datetime import datetime
from typing import Tuple, Optional
from pyspark.sql import SparkSession
import logging

from src.logger import ETLLogger
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.config_loader import ConfigLoader


class ETLOrchestrator:
    """
    Main ETL orchestrator that generates unique run IDs, 
    initializes components, and coordinates execution flow.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize orchestrator with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = ConfigLoader.load_config(config_path)
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            config=self.config
        )
        
        # Initialize ETL components
        self.extractor = Extractor(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        self.transformer = Transformer(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        self.loader = Loader(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID with format: ETL + 14-digit timestamp.
        
        Returns:
            Unique ETL run ID (e.g., ETL20240115143022)
        """
        prefix = self.config.get("run_id", {}).get("prefix", "ETL")
        timestamp_format = self.config.get("run_id", {}).get(
            "timestamp_format", "%Y%m%d%H%M%S"
        )
        timestamp = datetime.now().strftime(timestamp_format)
        run_id = f"{prefix}{timestamp}"
        
        return run_id
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.
        
        Returns:
            Configured SparkSession
        """
        spark_config = self.config.get("spark", {})
        app_name = spark_config.get("app_name", "ETL_Orchestrator")
        master = spark_config.get("master", "local[*]")
        
        builder = SparkSession.builder \
            .appName(f"{app_name}_{self.etl_run_id}") \
            .master(master)
        
        # Apply additional Spark configurations
        for key, value in spark_config.get("config", {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        log_level = spark_config.get("log_level", "WARN")
        spark.sparkContext.setLogLevel(log_level)
        
        return spark
    
    def run_etl(
        self,
        from_date: str,
        to_date: str
    ) -> bool:
        """
        Execute complete ETL process with all phases.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
        
        Returns:
            True if ETL process completed successfully, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )
            
            # Phase 1: Extract
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            extract_success, raw_data = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if not extract_success or raw_data is None:
                raise Exception("Extraction phase failed")
            
            # Phase 2: Transform
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            transform_success, analytics_data = self.transformer.transform_data(
                raw_data=raw_data
            )
            
            if not transform_success or analytics_data is None:
                raise Exception("Transformation phase failed")
            
            # Phase 3: Load
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            load_success = self.loader.load_data(
                analytics_data=analytics_data
            )
            
            if not load_success:
                raise Exception("Load phase failed")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            logging.error(f"ETL process failed: {str(e)}", exc_info=True)
            return False
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """
        Display execution summary with timing and statistics.
        """
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time.isoformat() if self.start_time else 'N/A'}")
        print(f"End Time:      {self.end_time.isoformat() if self.end_time else 'N/A'}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)
        
        # Display log summary
        self.logger.display_summary()
    
    def cleanup(self) -> None:
        """
        Cleanup resources and close connections.
        """
        try:
            if self.spark:
                self.spark.stop()
                self.logger.log_message(
                    step="CLEANUP",
                    status="S",
                    message="Spark session stopped successfully"
                )
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")


def main():
    """
    Main execution function for standalone testing.
    """
    from datetime import timedelta
    
    # Calculate default date range
    today = datetime.now().date()
    from_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    
    print("=" * 70)
    print(" " * 20 + "Sales Data ETL Process")
    print("=" * 70)
    print(f"\nProcessing Date Range: {from_date} to {to_date}")
    print(f"Test Mode: No\n")
    
    orchestrator = None
    
    try:
        # Create orchestrator
        orchestrator = ETLOrchestrator(config_path="config.yaml")
        
        print(f"ETL Run ID: {orchestrator.get_etl_run_id()}\n")
        
        # Run ETL
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date
        )
        
        # Display results
        print("\n")
        
        if success:
            print("*** ETL Process Completed Successfully ***")
            orchestrator.display_summary()
        else:
            print("*** ETL Process Failed ***")
            print("Please check the error logs for details.")
        
    except Exception as e:
        print(f"\n*** Fatal Error ***")
        print(f"Error: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}", exc_info=True)
    
    finally:
        if orchestrator:
            orchestrator.cleanup()


if __name__ == "__main__":
    main()