"""
ETL Orchestrator module.

Coordinates the extraction, transformation, and loading phases
of the sales data ETL pipeline.
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from src.extractor import ETLExtractor
from src.transformer import ETLTransformer
from src.loader import ETLLoader
from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLOrchestrator:
    """Main orchestrator for the ETL process."""

    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the ETL orchestrator.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Python logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger

        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()

        # Initialize ETL logger
        self.etl_logger = ETLLogger(self.etl_run_id, spark, config)

        # Initialize ETL components
        self.extractor = ETLExtractor(spark, config, self.etl_logger)
        self.transformer = ETLTransformer(spark, config, self.etl_logger)
        self.loader = ETLLoader(spark, config, self.etl_logger)

        # Timing
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Statistics
        self.statistics = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': 0
        }

        # Log initialization
        self.logger.info("ETL orchestrator initialized")
        self.etl_logger.log_message(
            step=ETLConstants.STEP_INIT,
            status=ETLConstants.STATUS_SUCCESS,
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.

        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"

    def run_etl(self, from_date: datetime, to_date: datetime,
                test_mode: bool = False) -> bool:
        """
        Execute the complete ETL process.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            test_mode: Whether to run in test mode

        Returns:
            True if successful, False otherwise
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            self.logger.info(f"ETL process started at {self.start_time}")
            self.etl_logger.log_message(
                step=ETLConstants.STEP_START,
                status=ETLConstants.STATUS_SUCCESS,
                message=f"ETL process started at {self.start_time}"
            )

            # Phase 1: Extract
            self.logger.info("=" * 70)
            self.logger.info("EXTRACT Phase")
            self.logger.info("=" * 70)

            raw_data = self.extractor.extract_data(from_date, to_date)
            self.statistics['extracted'] = raw_data.count()

            if self.statistics['extracted'] == 0:
                self.logger.warning("No data extracted. ETL process completed with no records.")
                self.etl_logger.log_message(
                    step=ETLConstants.STEP_EXTRACT,
                    status=ETLConstants.STATUS_WARNING,
                    message="No data to process"
                )
                return True

            # Phase 2: Transform
            self.logger.info("")
            self.logger.info("=" * 70)
            self.logger.info("TRANSFORM Phase")
            self.logger.info("=" * 70)

            analytics_data = self.transformer.transform_data(raw_data, self.etl_run_id)
            self.statistics['transformed'] = analytics_data.count()

            # Phase 3: Load
            self.logger.info("")
            self.logger.info("=" * 70)
            self.logger.info("LOAD Phase")
            self.logger.info("=" * 70)

            load_success, loaded_count = self.loader.load_data(
                analytics_data,
                test_mode=test_mode
            )
            self.statistics['loaded'] = loaded_count

            if not load_success:
                raise RuntimeError("Load phase failed")

            # Capture end time
            self.end_time = datetime.now()

            self.logger.info(f"ETL process completed at {self.end_time}")
            self.etl_logger.log_message(
                step=ETLConstants.STEP_COMPLETE,
                status=ETLConstants.STATUS_SUCCESS,
                records_processed=self.statistics['extracted'],
                records_success=self.statistics['loaded'],
                message=f"ETL process completed successfully at {self.end_time}"
            )

            return True

        except Exception as e:
            self.end_time = datetime.now()
            self.logger.exception(f"ETL process failed: {e}")
            self.etl_logger.log_message(
                step=ETLConstants.STEP_ERROR,
                status=ETLConstants.STATUS_ERROR,
                message=f"ETL process failed: {str(e)}"
            )
            return False

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.

        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def display_summary(self):
        """Display ETL process summary."""
        self.logger.info("")
        self.logger.info("ETL Process Summary")
        self.logger.info("-" * 70)
        self.logger.info(f"  ETL Run ID:    {self.etl_run_id}")
        self.logger.info(f"  Start Time:    {self.start_time}")
        self.logger.info(f"  End Time:      {self.end_time}")

        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"  Duration:      {duration:.2f} seconds")

        self.logger.info("")
        self.logger.info("  Statistics:")
        self.logger.info(f"    Records Extracted:   {self.statistics['extracted']}")
        self.logger.info(f"    Records Transformed: {self.statistics['transformed']}")
        self.logger.info(f"    Records Loaded:      {self.statistics['loaded']}")
        self.logger.info(f"    Errors:              {self.statistics['errors']}")
        self.logger.info("-" * 70)