"""
ETL Orchestrator
Coordinates the complete ETL pipeline with logging
"""

from datetime import datetime, date
from typing import Optional
from pyspark.sql import SparkSession

from src.logger import ETLLogger, ETLStep, ETLStatus, generate_etl_run_id
from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader


class ETLOrchestrator:
    """Main ETL orchestrator coordinating all ETL components"""

    def __init__(self, spark: SparkSession):
        """
        Initialize ETL orchestrator

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.etl_run_id = generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id)
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Initialize components
        self.extractor = SalesDataExtractor(self.logger, spark)
        self.transformer = SalesDataTransformer(self.logger, spark)
        self.loader = SalesDataLoader(self.logger, spark)

        self.logger.log_success(
            ETLStep.INIT,
            f"ETL process initialized with run ID: {self.etl_run_id}",
            etl_run_id=self.etl_run_id
        )

    def run_etl(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None
    ) -> bool:
        """
        Execute complete ETL pipeline

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

            self.logger.log_info(
                ETLStep.INIT,
                f"ETL process started at {self.start_time}",
                from_date=str(from_date),
                to_date=str(to_date)
            )

            # Extract
            print("\n" + "="*60)
            print("EXTRACT Phase")
            print("="*60)
            raw_df = self.extractor.extract_data(from_date, to_date, source_path)

            if raw_df is None or raw_df.count() == 0:
                raise Exception("No data extracted")

            # Transform
            print("\n" + "="*60)
            print("TRANSFORM Phase")
            print("="*60)
            analytics_df = self.transformer.transform_data(raw_df)

            if analytics_df is None or analytics_df.count() == 0:
                raise Exception("Transformation produced no data")

            # Load
            print("\n" + "="*60)
            print("LOAD Phase")
            print("="*60)
            load_success = self.loader.load_data(analytics_df, target_path)

            if not load_success:
                raise Exception("Load phase failed")

            self.end_time = datetime.now()

            self.logger.log_success(
                ETLStep.COMPLETE,
                f"ETL process completed successfully at {self.end_time}",
                duration_seconds=(self.end_time - self.start_time).total_seconds()
            )

            return True

        except Exception as e:
            self.end_time = datetime.now()

            self.logger.log_error(
                ETLStep.ERROR,
                f"ETL process failed: {str(e)}",
                exception=e,
                duration_seconds=(self.end_time - self.start_time).total_seconds() if self.start_time else 0
            )

            return False

    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id

    def display_summary(self):
        """Display ETL execution summary"""
        print("\n" + "="*60)
        print("ETL Process Summary")
        print("="*60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")

        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")

        print("="*60)