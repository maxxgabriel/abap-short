"""
ETL Orchestrator Module
Main orchestrator that coordinates the ETL pipeline process
Migrated from ZCL_ETL_ORCHESTRATOR ABAP class
"""

from datetime import datetime
from typing import Optional
import sys

from pyspark.sql import SparkSession

from src.logger import ETLLogger
from src.extractor import ETLExtractor
from src.transformer import ETLTransformer
from src.loader import ETLLoader
from src.exceptions import ETLException


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates extract, transform, and load operations.
    Migrates ABAP sequential method calls to PySpark pipeline stages.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize ETL orchestrator with all pipeline components.
        
        Args:
            spark: Optional SparkSession. Creates new session if not provided.
        """
        # Initialize or use provided Spark session
        self.spark = spark or self._create_spark_session()
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(self.etl_run_id, self.spark)
        
        # Initialize ETL components (ABAP object instantiation -> Python constructors)
        self.extractor = ETLExtractor(self.logger, self.spark)
        self.transformer = ETLTransformer(self.logger, self.spark)
        self.loader = ETLLoader(self.logger, self.spark)
        
        # Track execution times
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        return (SparkSession.builder
                .appName("SalesETLOrchestrator")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID based on timestamp.
        Migrates ABAP generate_etl_run_id method.
        
        Returns:
            Unique ETL run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL pipeline: Extract -> Transform -> Load.
        Migrates ABAP method calls to Python pipeline stages with try-except error handling.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD format)
            to_date: End date for extraction (YYYY-MM-DD format)
            
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
            
            # ===================================================================
            # STEP 1: EXTRACT (ABAP mo_extractor->extract_data)
            # ===================================================================
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            raw_data_df = self.extractor.extract_data(from_date, to_date)
            
            if raw_data_df is None or raw_data_df.count() == 0:
                raise ETLException(
                    error_text="Extraction returned no data",
                    error_step="EXTRACT"
                )
            
            # ===================================================================
            # STEP 2: TRANSFORM (ABAP mo_transformer->transform_data)
            # ===================================================================
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            analytics_df = self.transformer.transform_data(raw_data_df)
            
            if analytics_df is None or analytics_df.count() == 0:
                raise ETLException(
                    error_text="Transformation returned no data",
                    error_step="TRANSFORM"
                )
            
            # ===================================================================
            # STEP 3: LOAD (ABAP mo_loader->load_data)
            # ===================================================================
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise ETLException(
                    error_text="Load operation failed",
                    error_step="LOAD"
                )
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )
            
            return True
            
        except ETLException as etl_error:
            # Handle ETL-specific exceptions
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(etl_error)}"
            )
            
            print(f"\n*** ETL Error: {str(etl_error)} ***", file=sys.stderr)
            return False
            
        except Exception as ex:
            # Handle unexpected exceptions (ABAP CATCH cx_root)
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"Unexpected error in ETL process: {str(ex)}"
            )
            
            print(f"\n*** Unexpected Error: {str(ex)} ***", file=sys.stderr)
            return False

    def get_etl_run_id(self) -> str:
        """
        Get current ETL run ID.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def display_summary(self) -> None:
        """
        Display ETL process execution summary.
        Migrates ABAP display_summary method.
        """
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        
        if self.start_time:
            print(f"Start Time:    {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.end_time:
            print(f"End Time:      {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Calculate duration
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)

    def cleanup(self) -> None:
        """Clean up resources and stop Spark session if created internally."""
        if self.spark:
            self.spark.stop()