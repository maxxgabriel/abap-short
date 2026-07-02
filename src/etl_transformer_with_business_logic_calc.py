===FILE: src/transform.py===
"""
PySpark ETL Transformer Module
Transforms raw sales data into analytics format with business logic calculations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from datetime import datetime
from typing import Tuple
import logging


class ETLTransformer:
    """
    Transforms raw sales data into analytics format with business rules:
    - Gross amount calculation
    - Tiered discount rules (5% for qty > 10, 10% for qty > 15)
    - Tax computation (8% on gross - discount)
    - Profit margin calculation
    - Sale categorization (HIGH >= 2000, MEDIUM >= 500, LOW < 500)
    """
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize transformer with Spark session, configuration, and logger
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary with business rules
            logger: Python logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
        # Business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics output data
        
        Returns:
            StructType schema for transformed analytics data
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
        ])
    
    def transform_data(
        self, 
        raw_df: DataFrame, 
        etl_run_id: str
    ) -> Tuple[DataFrame, dict]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: DataFrame with raw sales data
            etl_run_id: Unique identifier for this ETL run
            
        Returns:
            Tuple of (transformed DataFrame, statistics dictionary)
        """
        try:
            self.logger.info("Starting data transformation")
            start_time = datetime.now()
            
            # Count input records
            input_count = raw_df.count()
            self.logger.info(f"Input records: {input_count}")
            
            # Step 1: Calculate gross amount
            df_with_gross = raw_df.withColumn(
                "gross_amount",
                F.col("quantity") * F.col("unit_price")
            )
            
            # Step 2: Apply tiered discount rules
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier2)
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier1)
                ).otherwise(F.lit(0.0))
            )
            
            # Step 3: Calculate tax on (gross - discount)
            df_with_tax = df_with_discount.withColumn(
                "tax_amount",
                (F.col("gross_amount") - F.col("discount_amount")) * F.lit(self.tax_rate)
            )
            
            # Step 4: Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
            )
            
            # Step 5: Calculate profit margin
            # Cost = quantity * unit_price * cost_ratio
            # Profit = net_amount - cost
            # Profit Margin % = (profit / net_amount) * 100
            df_with_profit = df_with_net.withColumn(
                "cost",
                F.col("quantity") * F.col("unit_price") * F.lit(self.cost_ratio)
            ).withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    ((F.col("net_amount") - F.col("cost")) / F.col("net_amount")) * 100
                ).otherwise(F.lit(0.0))
            )
            
            # Step 6: Categorize sales
            df_with_category = df_with_profit.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            )
            
            # Step 7: Generate analytics ID and add metadata
            analytics_df = df_with_category.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL"),
                    F.col("trans_id"),
                    F.date_format(F.current_timestamp(), "HHmmss")
                )
            ).withColumn(
                "etl_run_id", F.lit(etl_run_id)
            ).withColumn(
                "loaded_at", F.current_timestamp()
            ).withColumn(
                "total_quantity", F.col("quantity")
            )
            
            # Step 8: Select final columns in correct order
            final_df = analytics_df.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                "total_quantity",
                "gross_amount",
                "net_amount",
                "discount_amount",
                "tax_amount",
                "currency",
                "sales_rep",
                "region",
                "profit_margin",
                "category",
                "etl_run_id",
                "loaded_at"
            )
            
            # Validate output
            output_count = final_df.count()
            
            # Calculate statistics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            statistics = {
                "step": "TRANSFORM",
                "status": "SUCCESS",
                "records_processed": input_count,
                "records_success": output_count,
                "records_error": input_count - output_count,
                "duration_seconds": duration,
                "message": f"Transformed {output_count} of {input_count} records"
            }
            
            self.logger.info(
                f"Transformation completed: {output_count} records in {duration:.2f}s"
            )
            
            return final_df, statistics
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            statistics = {
                "step": "TRANSFORM",
                "status": "ERROR",
                "records_processed": 0,
                "records_success": 0,
                "records_error": 0,
                "message": f"Transformation failed: {str(e)}"
            }
            raise RuntimeError(f"Transformation failed: {str(e)}") from e
    
    def validate_analytics_record(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records meet business rules
        
        Args:
            df: Analytics DataFrame to validate
            
        Returns:
            DataFrame with validation flag
        """
        validated_df = df.withColumn(
            "is_valid",
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return validated_df
    
    def get_transformation_summary(self, df: DataFrame) -> dict:
        """
        Generate summary statistics for transformed data
        
        Args:
            df: Transformed analytics DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            # Aggregate statistics
            summary = df.agg(
                F.count("*").alias("total_records"),
                F.sum("gross_amount").alias("total_gross"),
                F.sum("discount_amount").alias("total_discount"),
                F.sum("tax_amount").alias("total_tax"),
                F.sum("net_amount").alias("total_net"),
                F.avg("profit_margin").alias("avg_profit_margin")
            ).collect()[0]
            
            # Category breakdown
            category_counts = df.groupBy("category").count().collect()
            category_dict = {row["category"]: row["count"] for row in category_counts}
            
            return {
                "total_records": summary["total_records"],
                "total_gross_amount": float(summary["total_gross"] or 0),
                "total_discount_amount": float(summary["total_discount"] or 0),
                "total_tax_amount": float(summary["total_tax"] or 0),
                "total_net_amount": float(summary["total_net"] or 0),
                "avg_profit_margin": float(summary["avg_profit_margin"] or 0),
                "category_breakdown": category_dict
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to generate summary: {str(e)}")
            return {}


===FILE: src/extract.py===
"""
PySpark ETL Extractor Module
Extracts raw sales data from source table
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime
from typing import Tuple
import logging


class ETLExtractor:
    """
    Extracts raw sales data from source table based on date range
    """
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize extractor with Spark session, configuration, and logger
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
            logger: Python logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.source_table = config.get('source_table', 'zsales_raw')
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data
        
        Returns:
            StructType schema for raw sales data
        """
        return StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), False),
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str
    ) -> Tuple[DataFrame, dict]:
        """
        Extract raw sales data from source table for date range
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Tuple of (DataFrame with raw sales data, statistics dictionary)
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            start_time = datetime.now()
            
            # Read from source table
            # In production, this would be: spark.table(self.source_table)
            # For demonstration, create sample data
            raw_df = self._create_sample_data()
            
            # Filter by date range and status
            filtered_df = raw_df.filter(
                (F.col("trans_date") >= F.lit(from_date)) &
                (F.col("trans_date") <= F.lit(to_date)) &
                (F.col("status") == F.lit("N"))
            )
            
            # Cache for performance
            filtered_df.cache()
            record_count = filtered_df.count()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            statistics = {
                "step": "EXTRACT",
                "status": "SUCCESS",
                "records_processed": record_count,
                "records_success": record_count,
                "records_error": 0,
                "duration_seconds": duration,
                "message": f"Extracted {record_count} records successfully"
            }
            
            self.logger.info(
                f"Extraction completed: {record_count} records in {duration:.2f}s"
            )
            
            return filtered_df, statistics
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            statistics = {
                "step": "EXTRACT",
                "status": "ERROR",
                "records_processed": 0,
                "records_success": 0,
                "records_error": 0,
                "message": f"Extraction failed: {str(e)}"
            }
            raise RuntimeError(f"Extraction failed: {str(e)}") from e
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample raw sales data for demonstration
        
        Returns:
            DataFrame with sample sales data
        """
        schema = self.get_raw_sales_schema()
        
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000006", "2024-01-16", "CUST005", "PROD001", 8, 99.99, "USD", "Alice Brown", "NORTH", "N"),
            ("T000007", "2024-01-16", "CUST002", "PROD004", 25, 49.99, "USD", "John Doe", "EAST", "N"),
            ("T000008", "2024-01-16", "CUST006", "PROD003", 12, 299.99, "USD", "Bob Wilson", "WEST", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema)
    
    def read_from_jdbc(
        self, 
        jdbc_url: str, 
        table_name: str,
        properties: dict,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Read data from JDBC source (for actual database connection)
        
        Args:
            jdbc_url: JDBC connection URL
            table_name: Source table name
            properties: JDBC connection properties
            from_date: Start date filter
            to_date: End date filter
            
        Returns:
            DataFrame with extracted data
        """
        query = f"""
            (SELECT * FROM {table_name}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') AS raw_data
        """
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=properties
        )
        
        return df


===FILE: src/load.py===
"""
PySpark ETL Loader Module
Loads transformed analytics data into target table
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from typing import Tuple
import logging


class ETLLoader:
    """
    Loads transformed analytics data into target table with validation
    """
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize loader with Spark session, configuration, and logger
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
            logger: Python logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.target_table = config.get('target_table', 'zsales_analytics')
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(self, analytics_df: DataFrame) -> Tuple[bool, dict]:
        """
        Load analytics data into target table
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success flag, statistics dictionary)
        """
        try:
            self.logger.info("Starting data load")
            start_time = datetime.now()
            
            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            # Separate valid and invalid records
            valid_df = validated_df.filter(F.col("is_valid") == True)
            invalid_df = validated_df.filter(F.col("is_valid") == False)
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            total_count = valid_count + invalid_count
            
            self.logger.info(f"Valid records: {valid_count}, Invalid: {invalid_count}")
            
            # Log invalid records for debugging
            if invalid_count > 0:
                self.logger.warning(f"Found {invalid_count} invalid records")
                invalid_df.select("analytics_id", "customer_id", "product_id").show(10)
            
            # Remove validation column before writing
            load_df = valid_df.drop("is_valid")
            
            # Write to target table
            # In production, use appropriate write mode and partitioning
            load_df.write \
                .mode("append") \
                .format("parquet") \
                .partitionBy("trans_date") \
                .saveAsTable(self.target_table)
            
            # Update source table status (would be done via JDBC in production)
            # This marks records as processed
            self._update_source_status(load_df)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            statistics = {
                "step": "LOAD",
                "status": "SUCCESS",
                "records_processed": total_count,
                "records_success": valid_count,
                "records_error": invalid_count,
                "duration_seconds": duration,
                "message": f"Loaded {valid_count} of {total_count} records"
            }
            
            self.logger.info(
                f"Load completed: {valid_count} records in {duration:.2f}s"
            )
            
            return True, statistics
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            statistics = {
                "step": "LOAD",
                "status": "ERROR",
                "records_processed": 0,
                "records_success": 0,
                "records_error": 0,
                "message": f"Load failed: {str(e)}"
            }
            return False, statistics
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records meet business rules
        
        Args:
            df: Analytics DataFrame to validate
            
        Returns:
            DataFrame with validation flag added
        """
        validated_df = df.withColumn(
            "is_valid",
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return validated_df
    
    def _update_source_status(self, df: DataFrame):
        """
        Update source table to mark records as processed
        
        Args:
            df: DataFrame with loaded records
        """
        # In production, this would update the source table via JDBC
        # Example SQL: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (...)
        
        trans_ids = [row.analytics_id for row in df.select("analytics_id").collect()]
        self.logger.info(f"Marked {len(trans_ids)} source records as processed")
    
    def write_to_jdbc(
        self,
        df: DataFrame,
        jdbc_url: str,
        table_name: str,
        properties: dict,
        mode: str = "append"
    ):
        """
        Write data to JDBC target (for actual database connection)
        
        Args:
            df: DataFrame to write
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: JDBC connection properties
            mode: Write mode (append, overwrite, etc.)
        """
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )
        
        self.logger.info(f"Written to {table_name} via JDBC")


===FILE: src/orchestrator.py===
"""
PySpark ETL Orchestrator Module
Main orchestrator that coordinates the ETL process
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates extract, transform, and load phases
    """
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize orchestrator with Spark session and configuration
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(config, self.etl_run_id)
        self.py_logger = logging.getLogger(__name__)
        
        # Initialize ETL components
        self.extractor = ETLExtractor(spark, config, self.py_logger)
        self.transformer = ETLTransformer(spark, config, self.py_logger)
        self.loader = ETLLoader(spark, config, self.py_logger)
        
        # Track execution metadata
        self.start_time = None
        self.end_time = None
        self.statistics = {}
        
        self.logger.log_message(
            step="INIT",
            status="SUCCESS",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute complete ETL process
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Boolean indicating overall success
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="INFO",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Phase 1: Extract
            self.py_logger.info("=" * 60)
            self.py_logger.info("EXTRACT Phase")
            self.py_logger.info("=" * 60)
            
            raw_df, extract_stats = self.extractor.extract_data(from_date, to_date)
            self.statistics['extract'] = extract_stats
            
            self.logger.log_message(
                step=extract_stats['step'],
                status=extract_stats['status'],
                records_processed=extract_stats['records_processed'],
                records_success=extract_stats['records_success'],
                records_error=extract_stats['records_error'],
                message=extract_stats['message']
            )
            
            if extract_stats['records_success'] == 0:
                raise RuntimeError("No records extracted")
            
            # Phase 2: Transform
            self.py_logger.info("=" * 60)
            self.py_logger.info("TRANSFORM Phase")
            self.py_logger.info("=" * 60)
            
            analytics_df, transform_stats = self.transformer.transform_data(
                raw_df, self.etl_run_id
            )
            self.statistics['transform'] = transform_stats
            
            self.logger.log_message(
                step=transform_stats['step'],
                status=transform_stats['status'],
                records_processed=transform_stats['records_processed'],
                records_success=transform_stats['records_success'],
                records_error=transform_stats['records_error'],
                message=transform_stats['message']
            )
            
            # Get transformation summary
            summary = self.transformer.get_transformation_summary(analytics_df)
            self.statistics['summary'] = summary
            
            # Phase 3: Load
            self.py_logger.info("=" * 60)
            self.py_logger.info("LOAD Phase")
            self.py_logger.info("=" * 60)
            
            load_success, load_stats = self.loader.load_data(analytics_df)
            self.statistics['load'] = load_stats
            
            self.logger.log_message(
                step=load_stats['step'],
                status=load_stats['status'],
                records_processed=load_stats['records_processed'],
                records_success=load_stats['records_success'],
                records_error=load_stats['records_error'],
                message=load_stats['message']
            )
            
            if not load_success:
                raise RuntimeError("Load phase failed")
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.log_message(
                step="COMPLETE",
                status="SUCCESS",
                message=f"ETL process completed successfully in {duration:.2f}s"
            )
            
            self.py_logger.info("=" * 60)
            self.py_logger.info("ETL Process Completed Successfully")
            self.py_logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="ERROR",
                message=f"ETL process failed: {str(e)}"
            )
            
            self.py_logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            return False
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}_{unique_id}"
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def display_summary(self):
        """
        Display execution summary
        """
        print("\n" + "=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("\n--- Phase Statistics ---")
        
        if 'extract' in self.statistics:
            stats = self.statistics['extract']
            print(f"\nExtract:")
            print(f"  Records: {stats['records_success']}")
            print(f"  Duration: {stats['duration_seconds']:.2f}s")
        
        if 'transform' in self.statistics:
            stats = self.statistics['transform']
            print(f"\nTransform:")
            print(f"  Records: {stats['records_success']}")
            print(f"  Errors: {stats['records_error']}")
            print(f"  Duration: {stats['duration_seconds']:.2f}s")
        
        if 'summary' in self.statistics:
            summary = self.statistics['summary']
            print(f"\nBusiness Metrics:")
            print(f"  Total Gross: ${summary.get('total_gross_amount', 0):,.