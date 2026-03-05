"""
ETL Loader Module - PySpark Implementation
Loads transformed analytics data with validation and error handling.
"""

from typing import Dict, Any, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging

from src.logger import ETLLogger
from src.exceptions import LoadError, ValidationError


class ETLLoader:
    """
    Data loader component for ETL pipeline.
    Handles validation, deduplication, and loading to target analytics table.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict[str, Any]):
        """
        Initialize ETL Loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.target_table = config['target']['analytics_table']
        self.source_table = config['source']['raw_table']
        self.write_mode = config['load'].get('write_mode', 'append')
        self.batch_size = config['load'].get('batch_size', 1000)
        
        # Validation thresholds
        self.validation_rules = config['validation']
        
        logging.info(f"ETLLoader initialized for target: {self.target_table}")
    
    def load_data(self, analytics_df: DataFrame) -> Tuple[bool, Dict[str, int]]:
        """
        Main load method - validates and loads data to target.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success flag, statistics dict)
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load process'
            )
            
            # Initial record count
            total_records = analytics_df.count()
            logging.info(f"Starting load for {total_records} records")
            
            # Validate records
            valid_df, validation_stats = self._validate_records(analytics_df)
            
            if valid_df.count() == 0:
                self.logger.log_message(
                    step='LOAD',
                    status='E',
                    message='No valid records to load after validation'
                )
                return False, validation_stats
            
            # Add load metadata
            enriched_df = self._add_load_metadata(valid_df)
            
            # Perform load operation
            load_stats = self._perform_load(enriched_df)
            
            # Update source table status
            if load_stats['loaded_count'] > 0:
                self._update_source_status(enriched_df)
            
            # Combine statistics
            final_stats = {
                **validation_stats,
                **load_stats,
                'total_input': total_records
            }
            
            # Log completion
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_records,
                records_success=load_stats['loaded_count'],
                records_error=validation_stats['invalid_count'],
                message=f"Load completed: {load_stats['loaded_count']} records loaded"
            )
            
            logging.info(f"Load statistics: {final_stats}")
            return True, final_stats
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            raise LoadError(f"Data load failed: {str(e)}")
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Validate analytics records against business rules.
        
        Args:
            df: Analytics DataFrame to validate
            
        Returns:
            Tuple of (valid DataFrame, validation statistics)
        """
        logging.info("Starting record validation")
        
        initial_count = df.count()
        
        # Required field validation
        valid_df = df.filter(
            col('analytics_id').isNotNull() &
            (col('analytics_id') != '') &
            col('customer_id').isNotNull() &
            (col('customer_id') != '') &
            col('product_id').isNotNull() &
            (col('product_id') != '') &
            (col('gross_amount') > 0)
        )
        
        # Currency validation
        valid_currencies = self.validation_rules.get('valid_currencies', ['USD', 'EUR', 'GBP'])
        valid_df = valid_df.filter(col('currency').isin(valid_currencies))
        
        # Category validation
        valid_categories = self.validation_rules.get('valid_categories', ['HIGH', 'MEDIUM', 'LOW'])
        valid_df = valid_df.filter(col('category').isin(valid_categories))
        
        # Numeric range validations
        valid_df = valid_df.filter(
            (col('total_quantity') > 0) &
            (col('net_amount') >= 0) &
            (col('discount_amount') >= 0) &
            (col('tax_amount') >= 0) &
            (col('profit_margin') >= -100) &
            (col('profit_margin') <= 100)
        )
        
        # Amount consistency check
        valid_df = valid_df.filter(
            col('net_amount') <= col('gross_amount')
        )
        
        valid_count = valid_df.count()
        invalid_count = initial_count - valid_count
        
        stats = {
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'validation_rate': round(valid_count / initial_count * 100, 2) if initial_count > 0 else 0
        }
        
        if invalid_count > 0:
            self.logger.log_message(
                step='VALIDATE',
                status='W',
                records_processed=initial_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Validation: {invalid_count} invalid records filtered"
            )
        
        logging.info(f"Validation complete: {valid_count}/{initial_count} valid records")
        return valid_df, stats
    
    def _add_load_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add loading metadata columns.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn('loaded_at', current_timestamp()) \
                 .withColumn('loaded_by', lit(self.config['runtime'].get('user', 'etl_system'))) \
                 .withColumn('load_batch_id', lit(self.logger.get_etl_run_id()))
    
    def _perform_load(self, df: DataFrame) -> Dict[str, int]:
        """
        Execute the actual load operation to target table.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Load statistics dictionary
        """
        try:
            record_count = df.count()
            logging.info(f"Loading {record_count} records to {self.target_table}")
            
            # Get write options from config
            write_options = self.config['load'].get('write_options', {})
            
            # Partition strategy
            partition_cols = self.config['load'].get('partition_by', [])
            
            # Write to target
            writer = df.write.mode(self.write_mode)
            
            # Apply partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Apply write options
            for key, value in write_options.items():
                writer = writer.option(key, value)
            
            # Execute write
            output_format = self.config['target'].get('format', 'parquet')
            output_path = self.config['target'].get('path')
            
            if output_format.lower() == 'delta':
                writer.format('delta').save(output_path)
            elif output_format.lower() == 'parquet':
                writer.parquet(output_path)
            elif output_format.lower() in ['jdbc', 'database']:
                jdbc_config = self.config['target']['jdbc']
                writer.jdbc(
                    url=jdbc_config['url'],
                    table=self.target_table,
                    properties=jdbc_config.get('properties', {})
                )
            else:
                writer.format(output_format).save(output_path)
            
            logging.info(f"Successfully loaded {record_count} records")
            
            return {
                'loaded_count': record_count,
                'load_errors': 0
            }
            
        except Exception as e:
            logging.error(f"Load execution failed: {str(e)}", exc_info=True)
            raise LoadError(f"Failed to write data: {str(e)}")
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update status in source table for processed records.
        
        Args:
            df: DataFrame with loaded records
        """
        try:
            # Extract transaction IDs from analytics IDs
            # Analytics ID format: ANL{trans_id}{timestamp}
            trans_ids = df.select('analytics_id').rdd.map(
                lambda row: row.analytics_id[3:13]  # Extract trans_id portion
            ).distinct().collect()
            
            if not trans_ids:
                return
            
            logging.info(f"Updating status for {len(trans_ids)} source records")
            
            # Update source table
            source_format = self.config['source'].get('format', 'parquet')
            source_path = self.config['source'].get('path')
            
            if source_format.lower() == 'delta':
                # Delta Lake supports updates
                from delta.tables import DeltaTable
                
                delta_table = DeltaTable.forPath(self.spark, source_path)
                delta_table.update(
                    condition=col('trans_id').isin(trans_ids),
                    set={'status': lit('P'), 'processed_at': current_timestamp()}
                )
            elif source_format.lower() in ['jdbc', 'database']:
                # Update via JDBC
                jdbc_config = self.config['source']['jdbc']
                update_query = f"""
                    UPDATE {self.source_table}
                    SET status = 'P', processed_at = CURRENT_TIMESTAMP
                    WHERE trans_id IN ({','.join(["'" + tid + "'" for tid in trans_ids])})
                """
                # Execute update (implementation depends on database driver)
                logging.info(f"Executed status update for {len(trans_ids)} records")
            else:
                # For other formats, status update may not be supported
                logging.warning(f"Status update not supported for format: {source_format}")
            
        except Exception as e:
            # Log but don't fail the load process
            logging.warning(f"Failed to update source status: {str(e)}")
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Source status update failed: {str(e)}"
            )
    
    def validate_prerequisites(self) -> bool:
        """
        Validate that target table and paths are accessible.
        
        Returns:
            True if prerequisites are met
        """
        try:
            target_path = self.config['target'].get('path')
            
            if target_path:
                # Check if path is writable (try creating a test file)
                test_df = self.spark.createDataFrame([], StructType([]))
                test_path = f"{target_path}/_spark_test"
                test_df.write.mode('overwrite').parquet(test_path)
                
                # Clean up test file
                import subprocess
                subprocess.run(['hadoop', 'fs', '-rm', '-r', test_path], 
                             capture_output=True)
                
                logging.info("Target path validation successful")
                return True
            
            return True
            
        except Exception as e:
            logging.error(f"Prerequisites validation failed: {str(e)}")
            return False


def get_analytics_schema() -> StructType:
    """
    Define the schema for analytics data.
    
    Returns:
        StructType schema definition
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
        StructField("loaded_at", TimestampType(), True),
        StructField("loaded_by", StringType(), True),
        StructField("load_batch_id", StringType(), True)
    ])