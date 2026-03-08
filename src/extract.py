"""
Data Extraction Module with Source Connectivity
Supports JDBC for SAP tables and Delta/Parquet files with secure credential handling
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)
from typing import Optional, Dict, Any
from datetime import date
import logging
from pathlib import Path


class DataExtractor:
    """
    PySpark data extraction component supporting multiple source types.
    Implements secure credential handling and date range filtering.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the DataExtractor.

        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary with connection parameters
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data matching SAP ZSALES_RAW table structure.

        Returns:
            StructType: Schema definition for raw sales data
        """
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
        ])

    def extract_from_jdbc(
        self,
        from_date: date,
        to_date: date,
        table_name: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from SAP database via JDBC connection.

        Args:
            from_date: Start date for filtering
            to_date: End date for filtering
            table_name: Optional table name override

        Returns:
            DataFrame: Extracted sales data

        Raises:
            ValueError: If JDBC configuration is missing
            Exception: If extraction fails
        """
        jdbc_config = self.config.get("jdbc", {})
        if not jdbc_config.get("url"):
            raise ValueError("JDBC URL not configured")

        table = table_name or jdbc_config.get("source_table", "ZSALES_RAW")

        self.logger.info(
            f"Extracting from JDBC source: {table} "
            f"(Date range: {from_date} to {to_date})"
        )

        # Build SQL query with date filtering
        query = f"""
            (SELECT 
                trans_id,
                trans_date,
                customer_id,
                product_id,
                quantity,
                unit_price,
                currency,
                sales_rep,
                region,
                status
            FROM {table}
            WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
              AND status = 'N') AS sales_data
        """

        jdbc_options = {
            "url": jdbc_config["url"],
            "dbtable": query,
            "driver": jdbc_config.get("driver", "com.sap.db.jdbc.Driver"),
            "fetchsize": str(jdbc_config.get("fetch_size", 1000)),
        }

        # Add authentication credentials securely
        if jdbc_config.get("user"):
            jdbc_options["user"] = jdbc_config["user"]
        if jdbc_config.get("password"):
            jdbc_options["password"] = jdbc_config["password"]

        # Add additional JDBC properties
        if jdbc_config.get("properties"):
            jdbc_options.update(jdbc_config["properties"])

        try:
            df = self.spark.read \
                .format("jdbc") \
                .options(**jdbc_options) \
                .load()

            record_count = df.count()
            self.logger.info(f"Successfully extracted {record_count} records from JDBC")

            return df

        except Exception as e:
            self.logger.error(f"JDBC extraction failed: {str(e)}")
            raise

    def extract_from_delta(
        self,
        from_date: date,
        to_date: date,
        path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from Delta Lake table.

        Args:
            from_date: Start date for filtering
            to_date: End date for filtering
            path: Optional Delta table path override

        Returns:
            DataFrame: Extracted sales data
        """
        delta_path = path or self.config.get("delta", {}).get("source_path")
        if not delta_path:
            raise ValueError("Delta source path not configured")

        self.logger.info(
            f"Extracting from Delta source: {delta_path} "
            f"(Date range: {from_date} to {to_date})"
        )

        try:
            df = self.spark.read \
                .format("delta") \
                .load(delta_path)

            # Apply date range filter
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == "N")
            )

            record_count = df_filtered.count()
            self.logger.info(f"Successfully extracted {record_count} records from Delta")

            return df_filtered

        except Exception as e:
            self.logger.error(f"Delta extraction failed: {str(e)}")
            raise

    def extract_from_parquet(
        self,
        from_date: date,
        to_date: date,
        path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from Parquet files.

        Args:
            from_date: Start date for filtering
            to_date: End date for filtering
            path: Optional Parquet path override

        Returns:
            DataFrame: Extracted sales data
        """
        parquet_path = path or self.config.get("parquet", {}).get("source_path")
        if not parquet_path:
            raise ValueError("Parquet source path not configured")

        self.logger.info(
            f"Extracting from Parquet source: {parquet_path} "
            f"(Date range: {from_date} to {to_date})"
        )

        try:
            df = self.spark.read \
                .schema(self.get_raw_sales_schema()) \
                .parquet(parquet_path)

            # Apply date range filter
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == "N")
            )

            record_count = df_filtered.count()
            self.logger.info(f"Successfully extracted {record_count} records from Parquet")

            return df_filtered

        except Exception as e:
            self.logger.error(f"Parquet extraction failed: {str(e)}")
            raise

    def extract(
        self,
        from_date: date,
        to_date: date,
        source_type: Optional[str] = None
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate source handler.

        Args:
            from_date: Start date for filtering
            to_date: End date for filtering
            source_type: Optional source type override ('jdbc', 'delta', 'parquet')

        Returns:
            DataFrame: Extracted sales data

        Raises:
            ValueError: If source type is invalid or not configured
        """
        source = source_type or self.config.get("source_type", "jdbc")

        self.logger.info(f"Starting extraction with source type: {source}")

        if source == "jdbc":
            return self.extract_from_jdbc(from_date, to_date)
        elif source == "delta":
            return self.extract_from_delta(from_date, to_date)
        elif source == "parquet":
            return self.extract_from_parquet(from_date, to_date)
        else:
            raise ValueError(f"Unsupported source type: {source}")

    def validate_extraction(self, df: DataFrame) -> bool:
        """
        Validate extracted data meets quality requirements.

        Args:
            df: Extracted DataFrame

        Returns:
            bool: True if validation passes
        """
        if df.isEmpty():
            self.logger.warning("Extracted DataFrame is empty")
            return False

        # Check for required columns
        required_columns = {
            "trans_id", "trans_date", "customer_id", "product_id",
            "quantity", "unit_price", "currency", "status"
        }
        actual_columns = set(df.columns)

        if not required_columns.issubset(actual_columns):
            missing = required_columns - actual_columns
            self.logger.error(f"Missing required columns: {missing}")
            return False

        # Check for null values in critical columns
        null_counts = df.select([
            df[col].isNull().cast("int").alias(col)
            for col in ["trans_id", "trans_date", "customer_id", "product_id"]
        ]).agg(*[
            sum(col).alias(col) for col in ["trans_id", "trans_date", "customer_id", "product_id"]
        ]).collect()[0].asDict()

        if any(count > 0 for count in null_counts.values()):
            self.logger.error(f"Found null values in critical columns: {null_counts}")
            return False

        self.logger.info("Extraction validation passed")
        return True