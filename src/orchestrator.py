"""
ETL Orchestrator Module
Coordinates the complete ETL pipeline with exception-based error handling
and UUID-based run ID generation.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame

from src.logger import ETLLogger
from src.extractor import ETLExtractor
from src.transformer import ETLTransformer
from src.loader import ETLLoader
from src.exceptions import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError
)


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    Manages the complete pipeline: Extract -> Transform -> Load
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the ETL orchestrator.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary with ETL parameters
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            log_level=config.get('log_level', 'INFO')
        )

        # Initialize ETL components
        self.extractor = ETLExtractor(
            spark=self.spark,
            logger=self.logger,
            config=config.get('extract', {})
        )

        self.transformer = ETLTransformer(
            spark=self.spark,
            logger=self.logger,
            config=config.get('transform', {})
        )

        self.loader = ETLLoader(
            spark=self.spark,
            logger=self.logger,
            config=config.get('load', {})
        )

        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID using UUID.

        Returns:
            Unique ETL run ID with ETL prefix
        """
        unique_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL_{timestamp}_{unique_id[:8]}"

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL pipeline.

        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)

        Returns:
            True if ETL process completed successfully, False otherwise

        Raises:
            ETLError: If any critical error occurs during processing
        """
        try:
            # Capture start time
            self.start_time = datetime.now()

            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )

            # Step 1: Extract
            logging.info("=== EXTRACT Phase ===")
            raw_data = self._execute_extract(from_date, to_date)

            if raw_data is None or raw_data.count() == 0:
                raise ExtractError(
                    error_text="No data extracted from source",
                    error_step='EXTRACT'
                )

            # Step 2: Transform
            logging.info("=== TRANSFORM Phase ===")
            analytics_data = self._execute_transform(raw_data)

            if analytics_data is None or analytics_data.count() == 0:
                raise TransformError(
                    error_text="No data produced from transformation",
                    error_step='TRANSFORM'
                )

            # Step 3: Load
            logging.info("=== LOAD Phase ===")
            load_success = self._execute_load(analytics_data)

            if not load_success:
                raise LoadError(
                    error_text="Data load failed",
                    error_step='LOAD'
                )

            # Capture end time
            self.end_time = datetime.now()

            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )

            return True

        except ExtractError as e:
            self._handle_error(e, 'EXTRACT')
            return False

        except TransformError as e:
            self._handle_error(e, 'TRANSFORM')
            return False

        except LoadError as e:
            self._handle_error(e, 'LOAD')
            return False

        except Exception as e:
            self._handle_error(
                ETLError(
                    error_text=f"Unexpected error: {str(e)}",
                    error_step='GENERAL'
                ),
                'GENERAL'
            )
            return False

    def _execute_extract(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Execute the extraction phase.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame containing raw sales data

        Raises:
            ExtractError: If extraction fails
        """
        try:
            return self.extractor.extract_data(from_date, to_date)

        except Exception as e:
            raise ExtractError(
                error_text=f"Extraction failed: {str(e)}",
                error_step='EXTRACT'
            )

    def _execute_transform(self, raw_data: DataFrame) -> Optional[DataFrame]:
        """
        Execute the transformation phase.

        Args:
            raw_data: Raw sales data DataFrame

        Returns:
            DataFrame containing transformed analytics data

        Raises:
            TransformError: If transformation fails
        """
        try:
            return self.transformer.transform_data(raw_data)

        except Exception as e:
            raise TransformError(
                error_text=f"Transformation failed: {str(e)}",
                error_step='TRANSFORM'
            )

    def _execute_load(self, analytics_data: DataFrame) -> bool:
        """
        Execute the load phase.

        Args:
            analytics_data: Transformed analytics data DataFrame

        Returns:
            True if load successful, False otherwise

        Raises:
            LoadError: If load fails
        """
        try:
            return self.loader.load_data(analytics_data)

        except Exception as e:
            raise LoadError(
                error_text=f"Load failed: {str(e)}",
                error_step='LOAD'
            )

    def _handle_error(self, error: ETLError, step: str):
        """
        Handle and log ETL errors.

        Args:
            error: ETL error instance
            step: Processing step where error occurred
        """
        if self.end_time is None:
            self.end_time = datetime.now()

        error_message = f"{step} failed: {error.error_text}"
        if error.record_id:
            error_message += f" (Record ID: {error.record_id})"

        self.logger.log_message(
            step='ERROR',
            status='E',
            message=error_message
        )

        logging.error(error_message)

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.

        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def display_summary(self) -> Dict[str, Any]:
        """
        Generate and display ETL process summary.

        Returns:
            Dictionary containing summary statistics
        """
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': None
        }

        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            summary['duration_seconds'] = duration

        # Log summary
        separator = "=" * 60
        logging.info(separator)
        logging.info("ETL Process Summary")
        logging.info(separator)
        logging.info(f"ETL Run ID:    {summary['etl_run_id']}")
        logging.info(f"Start Time:    {summary['start_time']}")
        logging.info(f"End Time:      {summary['end_time']}")
        if summary['duration_seconds']:
            logging.info(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        logging.info(separator)

        return summary

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get detailed statistics from all ETL components.

        Returns:
            Dictionary containing statistics from each component
        """
        return {
            'etl_run_id': self.etl_run_id,
            'extract': self.extractor.get_statistics(),
            'transform': self.transformer.get_statistics(),
            'load': self.loader.get_statistics()
        }