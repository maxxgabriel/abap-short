"""
ETL Loader Module
Loads transformed analytics data into target storage.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
import yaml
import logging


class ETLLoader:
    """Loads transformed analytics data into target destination."""
    
    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        """
        Initialize loader with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config_path: Path to configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> Dict[str, Any]:
        """
        Load analytics data to target destination.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional target path (overrides config)
            mode: Write mode (append, overwrite, error, ignore)
            
        Returns:
            Dictionary with load results and statistics
        """
        self.logger.info("Starting data load")
        
        try:
            # Use provided path or get from config
            path = target_path or self.config['data_targets']['analytics_path']
            
            # Validate data before loading
            validation = self._validate_before_load(analytics_df)
            
            if not validation['is_valid']:
                self.logger.warning(f"Data validation issues: {validation}")
                # Filter out invalid records
                analytics_df = self._filter_valid_records(analytics_df)
            
            # Load based on target type
            if path.endswith('.parquet'):
                self._load_to_parquet(analytics_df, path, mode)
            elif path.endswith('.csv'):
                self._load_to_csv(analytics_df, path, mode)
            elif path.startswith('jdbc:'):
                self._load_to_database(analytics_df, path, mode)
            else:
                raise ValueError(f"Unsupported target type: {path}")
            
            record_count = analytics_df.count()
            
            load_results = {
                "success": True,
                "records_loaded": record_count,
                "target_path": path,
                "mode": mode,
                "validation": validation
            }
            
            self.logger.info(f"Successfully loaded {record_count} records to {path}")
            
            return load_results
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "records_loaded": 0
            }
    
    def _load_to_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str
    ) -> None:
        """
        Load data to Parquet format.
        
        Args:
            df: DataFrame to load
            path: Target path
            mode: Write mode
        """
        self.logger.info(f"Writing Parquet to {path}")
        
        partition_cols = self.config['data_targets'].get('partition_columns', [])
        
        writer = df.write.mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.parquet(path)
    
    def _load_to_csv(
        self,
        df: DataFrame,
        path: str,
        mode: str
    ) -> None:
        """
        Load data to CSV format.
        
        Args:
            df: DataFrame to load
            path: Target path
            mode: Write mode
        """
        self.logger.info(f"Writing CSV to {path}")
        
        df.write \
            .mode(mode) \
            .option("header", "true") \
            .csv(path)
    
    def _load_to_database(
        self,
        df: DataFrame,
        jdbc_url: str,
        mode: str
    ) -> None:
        """
        Load data to database via JDBC.
        
        Args:
            df: DataFrame to load
            jdbc_url: JDBC connection URL
            mode: Write mode
        """
        self.logger.info(f"Writing to database: {jdbc_url}")
        
        db_config = self.config['data_targets']['database']
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_config['table']) \
            .option("user", db_config.get('user', '')) \
            .option("password", db_config.get('password', '')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .mode(mode) \
            .save()
    
    def _validate_before_load(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate data before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validation results dictionary
        """
        total_records = df.count()
        
        # Check for nulls in mandatory fields
        null_checks = {
            "analytics_id": df.filter(F.col("analytics_id").isNull()).count(),
            "customer_id": df.filter(F.col("customer_id").isNull()).count(),
            "product_id": df.filter(F.col("product_id").isNull()).count(),
            "gross_amount": df.filter(F.col("gross_amount").isNull()).count(),
        }
        
        # Check for invalid amounts
        invalid_amounts = df.filter(F.col("gross_amount") <= 0).count()
        
        # Check for empty strings
        empty_strings = df.filter(
            (F.col("analytics_id") == "") |
            (F.col("customer_id") == "") |
            (F.col("product_id") == "")
        ).count()
        
        total_invalid = sum(null_checks.values()) + invalid_amounts + empty_strings
        
        return {
            "total_records": total_records,
            "null_checks": null_checks,
            "invalid_amounts": invalid_amounts,
            "empty_strings": empty_strings,
            "total_invalid": total_invalid,
            "is_valid": total_invalid == 0
        }
    
    def _filter_valid_records(self, df: DataFrame) -> DataFrame:
        """
        Filter out invalid records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        self.logger.info("Filtering invalid records")
        
        return df.filter(
            F.col("analytics_id").isNotNull() &
            (F.col("analytics_id") != "") &
            F.col("customer_id").isNotNull() &
            (F.col("customer_id") != "") &
            F.col("product_id").isNotNull() &
            (F.col("product_id") != "") &
            F.col("gross_amount").isNotNull() &
            (F.col("gross_amount") > 0)
        )
    
    def update_source_status(
        self,
        spark: SparkSession,
        trans_ids: list,
        status: str = "P"
    ) -> None:
        """
        Update status of processed records in source table.
        
        Args:
            spark: SparkSession
            trans_ids: List of transaction IDs to update
            status: New status value
        """
        self.logger.info(f"Updating status for {len(trans_ids)} records")
        
        # In production, this would execute UPDATE statement
        # For demonstration, log the update
        self.logger.info(f"Would update trans_ids {trans_ids[:5]}... to status '{status}'")