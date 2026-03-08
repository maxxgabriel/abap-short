"""
PySpark ETL Orchestrator Module
Main orchestrator class with run_etl method, UUID-based run ID generation,
and statistics collection using Spark aggregations with comprehensive error handling.
"""

import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractError, TransformError, LoadError


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the complete ETL pipeline.
    Generates UUID-based run IDs, collects statistics, and handles errors.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize ETL orchestrator with Spark session and configuration.

        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary with ETL parameters
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Initialize logger
        self.logger = ETLLogger(
            spark=spark,
            etl_run_id=self.etl_run_id,
            log_level=config.get('log_level', 'INFO')
        )

        # Initialize ETL components
        self.extractor = DataExtractor(spark=spark, logger=self.logger, config=config)
        self.transformer = DataTransformer(spark=spark, logger=self.logger, config=config)
        self.loader = DataLoader(spark=spark, logger=self.logger, config=config)

        # Statistics storage
        self.statistics: Dict[str, Any] = {
            'extract': {},
            'transform': {},
            'load': {},
            'overall': {}
        }

        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID using UUID4.

        Returns:
            Unique ETL run identifier
        """
        return f"ETL_{uuid.uuid4().hex[:16].upper()}"

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.

        Returns:
            Current ETL run identifier
        """
        return self.etl_run_id

    def run_etl(
        self,
        from_date: str,
        to_date: str,
        dry_run: bool = False
    ) -> bool:
        """
        Execute complete ETL pipeline with comprehensive error handling.

        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            dry_run: If True, run without committing data

        Returns:
            True if ETL succeeded, False otherwise
        """
        try:
            self.start_time = datetime.now()
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )

            # Phase 1: Extract
            raw_data_df = self._execute_extract(from_date, to_date)
            if raw_data_df is None:
                raise ExtractError("Extraction phase failed - no data returned")

            # Phase 2: Transform
            analytics_df = self._execute_transform(raw_data_df)
            if analytics_df is None:
                raise TransformError("Transformation phase failed - no data returned")

            # Phase 3: Load
            load_success = self._execute_load(analytics_df, dry_run)
            if not load_success:
                raise LoadError("Load phase failed")

            # Calculate overall statistics
            self._calculate_overall_statistics()

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully. Duration: {duration:.2f} seconds',
                records_processed=self.statistics['overall'].get('total_records', 0),
                records_success=self.statistics['overall'].get('success_records', 0),
                records_error=self.statistics['overall'].get('error_records', 0)
            )

            return True

        except ExtractError as ex:
            self._handle_error('EXTRACT', str(ex))
            return False
        except TransformError as tx:
            self._handle_error('TRANSFORM', str(tx))
            return False
        except LoadError as lx:
            self._handle_error('LOAD', str(lx))
            return False
        except Exception as e:
            self._handle_error('ERROR', f'Unexpected error: {str(e)}')
            return False
        finally:
            if self.end_time is None:
                self.end_time = datetime.now()

    def _execute_extract(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Execute extraction phase with statistics collection.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame with extracted data or None on failure
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )

            raw_data_df = self.extractor.extract_data(from_date, to_date)

            if raw_data_df is None or raw_data_df.count() == 0:
                raise ExtractError("No data extracted from source")

            # Collect extraction statistics using Spark aggregations
            extract_stats = raw_data_df.agg(
                F.count("*").alias("total_records"),
                F.countDistinct("trans_id").alias("unique_transactions"),
                F.sum("quantity").alias("total_quantity"),
                F.sum(F.col("quantity") * F.col("unit_price")).alias("total_value")
            ).collect()[0]

            self.statistics['extract'] = {
                'total_records': extract_stats['total_records'],
                'unique_transactions': extract_stats['unique_transactions'],
                'total_quantity': extract_stats['total_quantity'],
                'total_value': float(extract_stats['total_value']) if extract_stats['total_value'] else 0.0
            }

            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Extracted {extract_stats['total_records']} records successfully",
                records_processed=extract_stats['total_records'],
                records_success=extract_stats['total_records']
            )

            return raw_data_df

        except Exception as e:
            raise ExtractError(f"Extraction failed: {str(e)}")

    def _execute_transform(self, raw_data_df: DataFrame) -> Optional[DataFrame]:
        """
        Execute transformation phase with statistics collection.

        Args:
            raw_data_df: Raw data DataFrame to transform

        Returns:
            Transformed analytics DataFrame or None on failure
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )

            analytics_df = self.transformer.transform_data(raw_data_df)

            if analytics_df is None or analytics_df.count() == 0:
                raise TransformError("No data produced by transformation")

            # Collect transformation statistics using Spark aggregations
            transform_stats = analytics_df.agg(
                F.count("*").alias("total_records"),
                F.sum("total_quantity").alias("total_quantity"),
                F.sum("gross_amount").alias("total_gross"),
                F.sum("net_amount").alias("total_net"),
                F.sum("discount_amount").alias("total_discount"),
                F.sum("tax_amount").alias("total_tax"),
                F.avg("profit_margin").alias("avg_profit_margin")
            ).collect()[0]

            # Category distribution
            category_stats = analytics_df.groupBy("category").count().collect()
            category_distribution = {row['category']: row['count'] for row in category_stats}

            self.statistics['transform'] = {
                'total_records': transform_stats['total_records'],
                'total_quantity': transform_stats['total_quantity'],
                'total_gross': float(transform_stats['total_gross']) if transform_stats['total_gross'] else 0.0,
                'total_net': float(transform_stats['total_net']) if transform_stats['total_net'] else 0.0,
                'total_discount': float(transform_stats['total_discount']) if transform_stats['total_discount'] else 0.0,
                'total_tax': float(transform_stats['total_tax']) if transform_stats['total_tax'] else 0.0,
                'avg_profit_margin': float(transform_stats['avg_profit_margin']) if transform_stats['avg_profit_margin'] else 0.0,
                'category_distribution': category_distribution
            }

            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message=f"Transformed {transform_stats['total_records']} records successfully",
                records_processed=transform_stats['total_records'],
                records_success=transform_stats['total_records']
            )

            return analytics_df

        except Exception as e:
            raise TransformError(f"Transformation failed: {str(e)}")

    def _execute_load(self, analytics_df: DataFrame, dry_run: bool) -> bool:
        """
        Execute load phase with statistics collection.

        Args:
            analytics_df: Analytics DataFrame to load
            dry_run: If True, skip actual data loading

        Returns:
            True if load succeeded, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )

            success_count, error_count = self.loader.load_data(
                analytics_df=analytics_df,
                dry_run=dry_run
            )

            # Collect load statistics
            self.statistics['load'] = {
                'total_records': success_count + error_count,
                'success_records': success_count,
                'error_records': error_count,
                'success_rate': (success_count / (success_count + error_count) * 100) if (success_count + error_count) > 0 else 0.0
            }

            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f"Loaded {success_count} of {success_count + error_count} records",
                records_processed=success_count + error_count,
                records_success=success_count,
                records_error=error_count
            )

            return error_count == 0

        except Exception as e:
            raise LoadError(f"Load failed: {str(e)}")

    def _calculate_overall_statistics(self) -> None:
        """
        Calculate overall ETL statistics by aggregating phase statistics.
        """
        duration_seconds = 0
        if self.start_time and self.end_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()

        self.statistics['overall'] = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration_seconds,
            'total_records': self.statistics.get('extract', {}).get('total_records', 0),
            'success_records': self.statistics.get('load', {}).get('success_records', 0),
            'error_records': self.statistics.get('load', {}).get('error_records', 0),
            'total_value': self.statistics.get('extract', {}).get('total_value', 0.0),
            'total_net': self.statistics.get('transform', {}).get('total_net', 0.0),
            'total_discount': self.statistics.get('transform', {}).get('total_discount', 0.0),
            'total_tax': self.statistics.get('transform', {}).get('total_tax', 0.0),
            'avg_profit_margin': self.statistics.get('transform', {}).get('avg_profit_margin', 0.0),
            'category_distribution': self.statistics.get('transform', {}).get('category_distribution', {})
        }

    def _handle_error(self, step: str, error_message: str) -> None:
        """
        Handle ETL errors with logging and cleanup.

        Args:
            step: ETL step where error occurred
            error_message: Error message to log
        """
        self.end_time = datetime.now()
        self.logger.log_message(
            step=step,
            status='E',
            message=f'ETL process failed: {error_message}'
        )

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get collected ETL statistics.

        Returns:
            Dictionary containing all ETL statistics
        """
        return self.statistics

    def display_summary(self) -> str:
        """
        Generate human-readable summary of ETL execution.

        Returns:
            Formatted summary string
        """
        stats = self.statistics.get('overall', {})
        duration = stats.get('duration_seconds', 0)

        summary = [
            "=" * 70,
            "ETL Process Summary",
            "=" * 70,
            f"ETL Run ID:       {stats.get('etl_run_id', 'N/A')}",
            f"Start Time:       {stats.get('start_time', 'N/A')}",
            f"End Time:         {stats.get('end_time', 'N/A')}",
            f"Duration:         {duration:.2f} seconds",
            "",
            "Record Statistics:",
            f"  Total Records:    {stats.get('total_records', 0)}",
            f"  Success Records:  {stats.get('success_records', 0)}",
            f"  Error Records:    {stats.get('error_records', 0)}",
            "",
            "Financial Metrics:",
            f"  Total Value:      ${stats.get('total_value', 0.0):,.2f}",
            f"  Total Net:        ${stats.get('total_net', 0.0):,.2f}",
            f"  Total Discount:   ${stats.get('total_discount', 0.0):,.2f}",
            f"  Total Tax:        ${stats.get('total_tax', 0.0):,.2f}",
            f"  Avg Profit:       {stats.get('avg_profit_margin', 0.0):.2f}%",
            "",
            "Category Distribution:"
        ]

        for category, count in stats.get('category_distribution', {}).items():
            summary.append(f"  {category:10s}: {count:5d} records")

        summary.append("=" * 70)

        return "\n".join(summary)