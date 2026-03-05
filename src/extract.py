"""
Data Extraction Module

Extracts raw sales data from source systems.
Converted from ABAP ZCL_ETL_EXTRACTOR class.
"""

from datetime import datetime
from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.schemas import ETLSchemas, ETLConstants
from src.logger import ETLLogger


class ETLExtractor:
    """
    Handles extraction of raw sales data from source systems.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor.

        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def extract_data(
        self,
        from_date: str,
        to_date: str,
        status_filter: str = ETLConstants.Status.NEW
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Extract raw sales data for the specified date range.

        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            status_filter: Status to filter on (default: 'N' for new records)

        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # Get data source configuration
            source_config = self.config.get("data_sources", {}).get("raw_sales", {})
            source_format = source_config.get("format", "parquet")

            # Extract based on source format
            if source_format == "jdbc":
                df = self._extract_from_jdbc(
                    source_config, from_date, to_date, status_filter
                )
            elif source_format in ["parquet", "delta", "csv"]:
                df = self._extract_from_file(
                    source_config, source_format, from_date, to_date, status_filter
                )
            else:
                raise ValueError(f"Unsupported source format: {source_format}")

            # Apply schema validation
            if self.config.get("data_quality", {}).get("validation", {}).get(
                "enable_schema_validation", True
            ):
                df = self._validate_and_cast_schema(df)

            # Add metadata columns
            df = self._add_extraction_metadata(df)

            record_count = df.count()

            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return df, True

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return None, False

    def _extract_from_jdbc(
        self,
        config: dict,
        from_date: str,
        to_date: str,
        status_filter: str
    ) -> DataFrame:
        """
        Extract data from JDBC source (e.g., database table).

        Args:
            config: JDBC configuration
            from_date: Start date
            to_date: End date
            status_filter: Status filter value

        Returns:
            DataFrame with extracted data
        """
        jdbc_url = config.get("jdbc_url")
        table = config.get("table")
        user = config.get("user")
        password = config.get("password")

        # Build query with filters
        query = f"""
            (SELECT *
             FROM {table}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
               AND status = '{status_filter}') as sales_data
        """

        df = (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", user)
            .option("password", password)
            .load()
        )

        return df

    def _extract_from_file(
        self,
        config: dict,
        file_format: str,
        from_date: str,
        to_date: str,
        status_filter: str
    ) -> DataFrame:
        """
        Extract data from file-based source.

        Args:
            config: File source configuration
            file_format: File format (parquet, delta, csv)
            from_date: Start date
            to_date: End date
            status_filter: Status filter value

        Returns:
            DataFrame with extracted data
        """
        path = config.get("path")

        # Read data based on format
        if file_format == "delta":
            df = self.spark.read.format("delta").load(path)
        elif file_format == "csv":
            df = (
                self.spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path)
            )
        else:  # parquet
            df = self.spark.read.parquet(path)

        # Apply filters
        df = df.filter(
            (F.col("trans_date") >= from_date) &
            (F.col("trans_date") <= to_date) &
            (F.col("status") == status_filter)
        )

        return df

    def _validate_and_cast_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate and cast DataFrame to expected schema.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with corrected schema
        """
        expected_schema = ETLSchemas.RAW_SALES_SCHEMA

        # Get expected field names
        expected_fields = {field.name for field in expected_schema.fields}
        actual_fields = set(df.columns)

        # Check for missing required fields
        missing_fields = expected_fields - actual_fields
        if missing_fields:
            raise ValueError(
                f"Missing required fields in source data: {missing_fields}"
            )

        # Select and cast to expected schema
        select_exprs = []
        for field in expected_schema.fields:
            if field.name in actual_fields:
                select_exprs.append(
                    F.col(field.name).cast(field.dataType).alias(field.name)
                )

        return df.select(*select_exprs)

    def _add_extraction_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add extraction metadata columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn(
            "extracted_at",
            F.current_timestamp()
        ).withColumn(
            "extracted_by",
            F.lit(self.config.get("etl", {}).get("user", "spark_etl"))
        )

    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing purposes.

        Returns:
            DataFrame with sample sales data
        """
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99,
             "USD", "John Doe", "NORTH", "N", datetime.now(), "spark_etl"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99,
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "spark_etl"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99,
             "USD", "John Doe", "EAST", "N", datetime.now(), "spark_etl"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99,
             "USD", "Bob Wilson", "WEST", "N", datetime.now(), "spark_etl"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99,
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "spark_etl"),
        ]

        return self.spark.createDataFrame(
            sample_data,
            schema=ETLSchemas.RAW_SALES_SCHEMA
        )