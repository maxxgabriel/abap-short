"""
Data Loading Module
Loads transformed analytics data to target destination
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from typing import Dict, Any, Optional
import logging


class DataLoader:
    """
    PySpark data loading component supporting multiple target types.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the DataLoader.

        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary with target parameters
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

    def load_to_jdbc(
        self,
        df: DataFrame,
        table_name: Optional[str] = None,
        mode: str = "append"
    ) -> int:
        """
        Load data to SAP database via JDBC connection.

        Args:
            df: DataFrame to load
            table_name: Optional target table name override
            mode: Write mode ('append', 'overwrite')

        Returns:
            int: Number of records loaded

        Raises:
            ValueError: If JDBC configuration is missing
        """
        jdbc_config = self.config.get("jdbc", {})
        if not jdbc_config.get("url"):
            raise ValueError("JDBC URL not configured")

        table = table_name or jdbc_config.get("target_table", "ZSALES_ANALYTICS")

        self.logger.info(f"Loading {df.count()} records to JDBC target: {table}")

        jdbc_options = {
            "url": jdbc_config["url"],
            "dbtable": table,
            "driver": jdbc_config.get("driver", "com.sap.db.jdbc.Driver"),
            "batchsize": str(jdbc_config.get("batch_size", 1000)),
        }

        # Add authentication credentials
        if jdbc_config.get("user"):
            jdbc_options["user"] = jdbc_config["user"]
        if jdbc_config.get("password"):
            jdbc_options["password"] = jdbc_config["password"]

        try:
            df.write \
                .format("jdbc") \
                .options(**jdbc_options) \
                .mode(mode) \
                .save()

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} records to JDBC")
            return record_count

        except Exception as e:
            self.logger.error(f"JDBC load failed: {str(e)}")
            raise

    def load_to_delta(
        self,
        df: DataFrame,
        path: Optional[str] = None,
        mode: str = "append"
    ) -> int:
        """
        Load data to Delta Lake table.

        Args:
            df: DataFrame to load
            path: Optional Delta table path override
            mode: Write mode ('append', 'overwrite')

        Returns:
            int: Number of records loaded
        """
        delta_path = path or self.config.get("delta", {}).get("target_path")
        if not delta_path:
            raise ValueError("Delta target path not configured")

        self.logger.info(f"Loading {df.count()} records to Delta target: {delta_path}")

        try:
            df.write \
                .format("delta") \
                .mode(mode) \
                .save(delta_path)

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} records to Delta")
            return record_count

        except Exception as e:
            self.logger.error(f"Delta load failed: {str(e)}")
            raise

    def load_to_parquet(
        self,
        df: DataFrame,
        path: Optional[str] = None,
        mode: str = "append",
        partition_by: Optional[list] = None
    ) -> int:
        """
        Load data to Parquet files.

        Args:
            df: DataFrame to load
            path: Optional Parquet path override
            mode: Write mode ('append', 'overwrite')
            partition_by: Optional list of columns to partition by

        Returns:
            int: Number of records loaded
        """
        parquet_path = path or self.config.get("parquet", {}).get("target_path")
        if not parquet_path:
            raise ValueError("Parquet target path not configured")

        self.logger.info(f"Loading {df.count()} records to Parquet target: {parquet_path}")

        try:
            writer = df.write.mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.parquet(parquet_path)

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} records to Parquet")
            return record_count

        except Exception as e:
            self.logger.error(f"Parquet load failed: {str(e)}")
            raise

    def update_source_status(
        self,
        processed_ids: list,
        status: str = "P"
    ) -> None:
        """
        Update status of processed records in source table.

        Args:
            processed_ids: List of transaction IDs to update
            status: New status value ('P' for processed)
        """
        jdbc_config = self.config.get("jdbc", {})
        source_table = jdbc_config.get("source_table", "ZSALES_RAW")

        if not processed_ids:
            self.logger.warning("No transaction IDs to update")
            return

        self.logger.info(f"Updating status for {len(processed_ids)} records in {source_table}")

        # In production, implement proper SQL UPDATE logic
        # This is a placeholder for the update operation
        self.logger.info(f"Status update would mark records as '{status}'")

    def load(
        self,
        df: DataFrame,
        target_type: Optional[str] = None
    ) -> int:
        """
        Main loading method that routes to appropriate target handler.

        Args:
            df: DataFrame to load
            target_type: Optional target type override ('jdbc', 'delta', 'parquet')

        Returns:
            int: Number of records loaded

        Raises:
            ValueError: If target type is invalid or not configured
        """
        target = target_type or self.config.get("target_type", "jdbc")

        self.logger.info(f"Starting load with target type: {target}")

        if target == "jdbc":
            return self.load_to_jdbc(df)
        elif target == "delta":
            return self.load_to_delta(df)
        elif target == "parquet":
            partition_cols = self.config.get("parquet", {}).get("partition_by", ["trans_date"])
            return self.load_to_parquet(df, partition_by=partition_cols)
        else:
            raise ValueError(f"Unsupported target type: {target}")

    def validate_load(self, df: DataFrame, records_loaded: int) -> bool:
        """
        Validate loaded data count matches expected count.

        Args:
            df: Source DataFrame
            records_loaded: Number of records reported as loaded

        Returns:
            bool: True if validation passes
        """
        expected_count = df.count()

        if records_loaded != expected_count:
            self.logger.error(
                f"Load count mismatch: expected {expected_count}, loaded {records_loaded}"
            )
            return False

        self.logger.info("Load validation passed")
        return True