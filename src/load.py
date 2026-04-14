"""
PySpark Data Loading Module
Loads transformed analytics data into target storage with partitioning and write modes.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Tuple
import logging
from datetime import datetime


class DataLoader:
    """Handles loading of transformed data into target analytics table."""
    
    def __init__(self, spark: SparkSession, config: Dict, logger: logging.Logger):
        """
        Initialize DataLoader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.load_config = config.get('load', {})
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema for analytics table
        """
        return StructType([
            StructField("analytics_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("total_quantity", IntegerType(), False),
            StructField("gross_amount", DecimalType(16, 2), False),
            StructField("net_amount", DecimalType(16, 2), False),
            StructField("discount_amount", DecimalType(16, 2), False),
            StructField("tax_amount", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), False)
        ])
    
    def validate_record(self, row: Dict) -> Tuple[bool, str]:
        """
        Validate a single analytics record.
        
        Args:
            row: Dictionary representing a record
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check required fields
        required_fields = ['analytics_id', 'customer_id', 'product_id', 'gross_amount']
        for field in required_fields:
            if not row.get(field):
                return False, f"Missing required field: {field}"
        
        # Validate gross amount
        if row.get('gross_amount', 0) <= 0:
            return False, "Gross amount must be positive"
        
        # Validate currency
        if not row.get('currency'):
            return False, "Currency is required"
        
        # Validate category
        valid_categories = ['HIGH', 'MEDIUM', 'LOW']
        if row.get('category') not in valid_categories:
            return False, f"Invalid category: {row.get('category')}"
        
        return True, ""
    
    def validate_dataframe(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate all records in DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        self.logger.info("Validating analytics records")
        
        # Add validation flag
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType
        
        def is_valid_record(analytics_id, customer_id, product_id, gross_amount, currency, category):
            if not analytics_id or not customer_id or not product_id:
                return False
            if not gross_amount or gross_amount <= 0:
                return False
            if not currency:
                return False
            if category not in ['HIGH', 'MEDIUM', 'LOW']:
                return False
            return True
        
        validate_udf = udf(is_valid_record, BooleanType())
        
        df_with_validation = df.withColumn(
            "is_valid",
            validate_udf(
                col("analytics_id"),
                col("customer_id"),
                col("product_id"),
                col("gross_amount"),
                col("currency"),
                col("category")
            )
        )
        
        valid_df = df_with_validation.filter(col("is_valid") == True).drop("is_valid")
        invalid_df = df_with_validation.filter(col("is_valid") == False).drop("is_valid")
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        self.logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid records")
        
        return valid_df, invalid_df
    
    def add_audit_columns(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with audit columns
        """
        return df.withColumn("loaded_at", current_timestamp()) \
                 .withColumn("loaded_by", lit(self.config.get('etl', {}).get('user', 'spark_etl'))) \
                 .withColumn("etl_run_id", lit(etl_run_id))
    
    def load_data(self, df: DataFrame, etl_run_id: str) -> Dict:
        """
        Load transformed data into target analytics table.
        
        Args:
            df: Transformed analytics DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Dictionary with load statistics
        """
        try:
            self.logger.info("Starting data load process")
            
            total_records = df.count()
            self.logger.info(f"Total records to load: {total_records}")
            
            # Validate records
            valid_df, invalid_df = self.validate_dataframe(df)
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            
            # Log invalid records if any
            if invalid_count > 0:
                self.logger.warning(f"Found {invalid_count} invalid records")
                invalid_path = self.load_config.get('invalid_records_path', 'output/invalid_records')
                invalid_df.write.mode('append').parquet(invalid_path)
                self.logger.info(f"Invalid records written to: {invalid_path}")
            
            # Add audit columns
            load_df = self.add_audit_columns(valid_df, etl_run_id)
            
            # Get write configuration
            write_mode = self.load_config.get('write_mode', 'append')
            output_path = self.load_config.get('output_path', 'output/analytics')
            partition_columns = self.load_config.get('partition_columns', ['trans_date', 'region'])
            output_format = self.load_config.get('format', 'parquet')
            
            self.logger.info(f"Writing data to: {output_path}")
            self.logger.info(f"Write mode: {write_mode}")
            self.logger.info(f"Partition columns: {partition_columns}")
            self.logger.info(f"Output format: {output_format}")
            
            # Write data with partitioning
            writer = load_df.write.mode(write_mode)
            
            # Add partitioning if specified
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            # Add additional write options
            if output_format == 'parquet':
                writer = writer.option('compression', self.load_config.get('compression', 'snappy'))
            
            # Execute write
            writer.format(output_format).save(output_path)
            
            self.logger.info(f"Successfully loaded {valid_count} records")
            
            # Update source table status (simulated)
            self._update_source_status(valid_df)
            
            return {
                'success': True,
                'total_records': total_records,
                'valid_records': valid_count,
                'invalid_records': invalid_count,
                'loaded_records': valid_count,
                'output_path': output_path,
                'message': f'Loaded {valid_count} of {total_records} records successfully'
            }
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'total_records': 0,
                'valid_records': 0,
                'invalid_records': 0,
                'loaded_records': 0,
                'output_path': '',
                'message': f'Load failed: {str(e)}'
            }
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update status in source table for processed records.
        
        Args:
            df: DataFrame with processed records
        """
        try:
            self.logger.info("Updating source table status")
            
            # In production, this would update the source table
            # For now, we'll write the transaction IDs to a status file
            status_path = self.load_config.get('status_update_path', 'output/processed_status')
            
            # Extract transaction IDs and write status update
            status_df = df.select(
                col("analytics_id"),
                lit("P").alias("status"),
                current_timestamp().alias("processed_at")
            )
            
            status_df.write.mode('append').parquet(status_path)
            
            self.logger.info(f"Status updates written to: {status_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to update source status: {str(e)}")
    
    def load_to_database(self, df: DataFrame, etl_run_id: str) -> Dict:
        """
        Load data to database using JDBC (alternative to file-based load).
        
        Args:
            df: Transformed analytics DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Dictionary with load statistics
        """
        try:
            self.logger.info("Starting database load process")
            
            # Validate records
            valid_df, invalid_df = self.validate_dataframe(df)
            valid_count = valid_df.count()
            
            # Add audit columns
            load_df = self.add_audit_columns(valid_df, etl_run_id)
            
            # Get database configuration
            db_config = self.load_config.get('database', {})
            jdbc_url = db_config.get('jdbc_url')
            table_name = db_config.get('table_name', 'sales_analytics')
            write_mode = db_config.get('write_mode', 'append')
            
            if not jdbc_url:
                raise ValueError("JDBC URL not configured")
            
            self.logger.info(f"Writing to database table: {table_name}")
            
            # Write to database
            load_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
                .mode(write_mode) \
                .save()
            
            self.logger.info(f"Successfully loaded {valid_count} records to database")
            
            return {
                'success': True,
                'loaded_records': valid_count,
                'message': f'Loaded {valid_count} records to database'
            }
            
        except Exception as e:
            self.logger.error(f"Database load failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'loaded_records': 0,
                'message': f'Database load failed: {str(e)}'
            }


def create_loader(spark: SparkSession, config: Dict, logger: logging.Logger) -> DataLoader:
    """
    Factory function to create DataLoader instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        DataLoader instance
    """
    return DataLoader(spark, config, logger)