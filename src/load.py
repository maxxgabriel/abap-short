"""
PySpark Data Loading Module
Loads transformed analytics data into target storage.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging


class SalesDataLoader:
    """Loads transformed analytics data into target storage."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> dict:
        """
        Load analytics data to target storage.
        
        Args:
            analytics_df: Analytics DataFrame to load
            target_path: Optional target path (overrides config)
            mode: Write mode (append, overwrite, error, ignore)
            
        Returns:
            Dictionary with load statistics
        """
        try:
            self.logger.info("Starting data load")
            
            # Validate data before loading
            validation_results = self._validate_before_load(analytics_df)
            
            if validation_results["invalid_records"] > 0:
                self.logger.warning(
                    f"Found {validation_results['invalid_records']} invalid records"
                )
            
            # Get target configuration
            path = target_path or self.config.get("target_path")
            target_format = self.config.get("target_format", "parquet")
            partition_cols = self.config.get("partition_columns", ["trans_date"])
            
            # Write data
            writer = analytics_df.write \
                .format(target_format) \
                .mode(mode)
            
            # Add partitioning if configured
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Add compression if configured
            compression = self.config.get("compression")
            if compression:
                writer = writer.option("compression", compression)
            
            writer.save(path)
            
            record_count = analytics_df.count()
            
            self.logger.info(
                f"Loaded {record_count} records to {path}"
            )
            
            return {
                "success": True,
                "records_loaded": record_count,
                "target_path": path,
                "format": target_format,
                "mode": mode,
                "validation": validation_results
            }
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def load_with_deduplication(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None
    ) -> dict:
        """
        Load data with deduplication based on analytics_id.
        
        Args:
            analytics_df: Analytics DataFrame
            target_path: Optional target path
            
        Returns:
            Load statistics dictionary
        """
        try:
            # Remove duplicates based on analytics_id
            deduplicated_df = analytics_df.dropDuplicates(["analytics_id"])
            
            original_count = analytics_df.count()
            deduplicated_count = deduplicated_df.count()
            duplicates_removed = original_count - deduplicated_count
            
            if duplicates_removed > 0:
                self.logger.warning(
                    f"Removed {duplicates_removed} duplicate records"
                )
            
            # Load deduplicated data
            result = self.load_data(deduplicated_df, target_path)
            result["duplicates_removed"] = duplicates_removed
            
            return result
            
        except Exception as e:
            self.logger.error(f"Deduplication load failed: {str(e)}")
            raise
    
    def load_incremental(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None
    ) -> dict:
        """
        Load data incrementally, updating existing records.
        
        Args:
            analytics_df: Analytics DataFrame
            target_path: Optional target path
            
        Returns:
            Load statistics dictionary
        """
        try:
            path = target_path or self.config.get("target_path")
            
            # Check if target exists
            try:
                existing_df = self.spark.read.parquet(path)
                
                # Merge logic: remove existing records with same analytics_id
                merged_df = existing_df.filter(
                    ~F.col("analytics_id").isin(
                        [row.analytics_id for row in analytics_df.select("analytics_id").collect()]
                    )
                ).union(analytics_df)
                
                # Overwrite with merged data
                result = self.load_data(merged_df, target_path, mode="overwrite")
                result["load_type"] = "incremental_merge"
                
            except Exception:
                # Target doesn't exist, do initial load
                result = self.load_data(analytics_df, target_path, mode="overwrite")
                result["load_type"] = "initial_load"
            
            return result
            
        except Exception as e:
            self.logger.error(f"Incremental load failed: {str(e)}")
            raise
    
    def load_to_multiple_targets(
        self,
        analytics_df: DataFrame,
        targets: list
    ) -> dict:
        """
        Load data to multiple target locations.
        
        Args:
            analytics_df: Analytics DataFrame
            targets: List of target configurations
            
        Returns:
            Dictionary with results for each target
        """
        results = {}
        
        for idx, target in enumerate(targets):
            try:
                target_name = target.get("name", f"target_{idx}")
                target_path = target.get("path")
                target_format = target.get("format", "parquet")
                mode = target.get("mode", "append")
                
                self.logger.info(f"Loading to target: {target_name}")
                
                analytics_df.write \
                    .format(target_format) \
                    .mode(mode) \
                    .save(target_path)
                
                results[target_name] = {
                    "success": True,
                    "path": target_path,
                    "format": target_format
                }
                
            except Exception as e:
                self.logger.error(f"Failed to load to {target_name}: {str(e)}")
                results[target_name] = {
                    "success": False,
                    "error": str(e)
                }
        
        return results
    
    def update_source_status(
        self,
        raw_df: DataFrame,
        processed_ids: list,
        source_path: str
    ) -> None:
        """
        Update status of processed records in source table.
        Simulates ABAP UPDATE statement.
        
        Args:
            raw_df: Original raw DataFrame
            processed_ids: List of processed transaction IDs
            source_path: Path to source data
        """
        try:
            # Update status to 'P' (Processed) for processed records
            updated_df = raw_df.withColumn(
                "status",
                F.when(
                    F.col("trans_id").isin(processed_ids),
                    F.lit("P")
                ).otherwise(F.col("status"))
            )
            
            # Write back to source (in practice, this might be a database update)
            updated_df.write \
                .format(self.config.get("source_format", "parquet")) \
                .mode("overwrite") \
                .save(source_path)
            
            self.logger.info(
                f"Updated status for {len(processed_ids)} records in source"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to update source status: {str(e)}")
            raise
    
    def _validate_before_load(self, analytics_df: DataFrame) -> dict:
        """
        Validate data before loading.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Validation results dictionary
        """
        total_records = analytics_df.count()
        
        # Check for required fields
        null_ids = analytics_df.filter(F.col("analytics_id").isNull()).count()
        null_customers = analytics_df.filter(F.col("customer_id").isNull()).count()
        null_products = analytics_df.filter(F.col("product_id").isNull()).count()
        
        # Check for invalid amounts
        invalid_gross = analytics_df.filter(F.col("gross_amount") <= 0).count()
        invalid_net = analytics_df.filter(F.col("net_amount") <= 0).count()
        
        # Check for invalid categories
        invalid_categories = analytics_df.filter(
            ~F.col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()
        
        invalid_records = (
            null_ids + null_customers + null_products +
            invalid_gross + invalid_net + invalid_categories
        )
        
        return {
            "total_records": total_records,
            "valid_records": total_records - invalid_records,
            "invalid_records": invalid_records,
            "null_ids": null_ids,
            "null_customers": null_customers,
            "null_products": null_products,
            "invalid_gross_amounts": invalid_gross,
            "invalid_net_amounts": invalid_net,
            "invalid_categories": invalid_categories
        }
    
    def create_summary_report(self, analytics_df: DataFrame) -> DataFrame:
        """
        Create summary report for loaded data.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Summary DataFrame
        """
        summary = analytics_df.agg(
            F.count("analytics_id").alias("total_transactions"),
            F.sum("total_quantity").alias("total_quantity_sold"),
            F.sum("gross_amount").alias("total_gross_amount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.sum("discount_amount").alias("total_discount"),
            F.sum("tax_amount").alias("total_tax"),
            F.avg("profit_margin").alias("avg_profit_margin"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("region").alias("regions_count")
        )
        
        return summary