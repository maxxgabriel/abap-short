"""
ETL Orchestrator Module
Coordinates the sequential execution of Extract, Transform, and Load operations
with comprehensive error propagation and timestamp tracking.
"""

from datetime import datetime
from typing import Tuple, Optional
import logging

from pyspark.sql import SparkSession, DataFrame

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractError, TransformError, LoadError


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    Manages sequential execution flow with proper error propagation.
    """

    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the ETL orchestrator.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Initialize logger
        self.logger = ETLLogger(self.etl_run_id)

        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger, config)
        self.transformer = SalesTransformer(self.logger, config)
        self.loader = SalesLoader(spark, self.logger, config)

        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.

        Returns:
            Unique ETL run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
        return f"ETL{timestamp}"

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process.

        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)

        Returns:
            True if ETL process completed successfully, False otherwise

        Raises:
            ETLError: If any ETL step fails and error propagation is enabled
        """
        try:
            # Capture start time
            self.start_time = datetime.now()

            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time.isoformat()}"
            )

            # Step 1: Extract
            print("\n=== EXTRACT Phase ===")
            raw_data = self._execute_extract(from_date, to_date)

            # Step 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_data = self._execute_transform(raw_data)

            # Step 3: Load
            print("\n=== LOAD Phase ===")
            self._execute_load(analytics_data)

            # Capture end time
            self.end_time = datetime.now()

            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time.isoformat()}"
            )

            return True

        except (ExtractError, TransformError, LoadError) as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            logging.error(f"ETL failed: {str(e)}", exc_info=True)

            # Propagate error if configured
            if self.config.get("error_propagation", {}).get("propagate_errors", True):
                raise ETLError(f"ETL process failed: {str(e)}") from e

            return False

        except Exception as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"Unexpected error: {str(e)}"
            )
            logging.error(f"Unexpected ETL error: {str(e)}", exc_info=True)

            # Always propagate unexpected errors
            raise ETLError(f"Unexpected ETL error: {str(e)}") from e

    def _execute_extract(self, from_date: str, to_date: str) -> DataFrame:
        """
        Execute extraction phase with error handling.

        Args:
            from_date: Start date
            to_date: End date

        Returns:
            Extracted DataFrame

        Raises:
            ExtractError: If extraction fails
        """
        try:
            raw_data = self.extractor.extract_data(from_date, to_date)

            if raw_data.isEmpty():
                raise ExtractError("No data extracted from source")

            return raw_data

        except Exception as e:
            raise ExtractError(f"Extraction failed: {str(e)}") from e

    def _execute_transform(self, raw_data: DataFrame) -> DataFrame:
        """
        Execute transformation phase with error handling.

        Args:
            raw_data: Raw data DataFrame

        Returns:
            Transformed DataFrame

        Raises:
            TransformError: If transformation fails
        """
        try:
            analytics_data = self.transformer.transform_data(raw_data)

            if analytics_data.isEmpty():
                raise TransformError("Transformation produced no records")

            return analytics_data

        except Exception as e:
            raise TransformError(f"Transformation failed: {str(e)}") from e

    def _execute_load(self, analytics_data: DataFrame) -> None:
        """
        Execute load phase with error handling.

        Args:
            analytics_data: Analytics data to load

        Raises:
            LoadError: If load fails
        """
        try:
            self.loader.load_data(analytics_data)

        except Exception as e:
            raise LoadError(f"Load failed: {str(e)}") from e

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.

        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def display_summary(self) -> dict:
        """
        Generate and display ETL process summary.

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": None,
            "status": "completed" if self.end_time else "in_progress"
        }

        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            summary["duration_seconds"] = duration

        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        if summary['duration_seconds']:
            print(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        print("=" * 60)

        return summary

    def get_execution_stats(self) -> dict:
        """
        Get detailed execution statistics.

        Returns:
            Dictionary with execution statistics
        """
        return {
            "etl_run_id": self.etl_run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "extractor_stats": self.extractor.get_stats(),
            "transformer_stats": self.transformer.get_stats(),
            "loader_stats": self.loader.get_stats()
        }