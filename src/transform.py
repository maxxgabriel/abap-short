"""
PySpark Data Transformation Engine with Business Logic
Implements calculated fields: gross_amount, net_amount, discount_amount, tax_amount, profit_margin
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Any
import logging
from datetime import datetime
import yaml


class TransformationEngine:
    """
    PySpark transformation engine implementing business logic for sales data.
    Calculates derived fields based on business rules from ABAP source.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize transformation engine with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_qty_tier1 = config['business_rules']['discount_qty_tier1']
        self.discount_qty_tier2 = config['business_rules']['discount_qty_tier2']
        self.discount_rate_tier1 = config['business_rules']['discount_rate_tier1']
        self.discount_rate_tier2 = config['business_rules']['discount_rate_tier2']
        self.tax_rate = config['business_rules']['tax_rate']
        self.cost_ratio = config['business_rules']['cost_ratio']
        self.category_high_threshold = config['business_rules']['category_high_threshold']
        self.category_medium_threshold = config['business_rules']['category_medium_threshold']
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data matching ABAP structure.
        
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
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics output matching ABAP target structure.
        
        Returns:
            StructType schema for analytics data
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
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), True)
        ])
    
    def calculate_gross_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate gross amount: quantity * unit_price
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with gross_amount column added
        """
        self.logger.info("Calculating gross_amount")
        return df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
        )
    
    def calculate_discount_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate discount based on quantity tiers:
        - quantity > 15: 10% discount
        - quantity > 10: 5% discount
        - else: no discount
        
        Args:
            df: Input DataFrame with gross_amount
            
        Returns:
            DataFrame with discount_amount column added
        """
        self.logger.info("Calculating discount_amount with tiered logic")
        return df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * self.discount_rate_tier2
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * self.discount_rate_tier1
            ).otherwise(
                F.lit(0)
            ).cast(DecimalType(16, 2))
        )
    
    def calculate_tax_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax: (gross_amount - discount_amount) * tax_rate
        Tax is applied after discount.
        
        Args:
            df: Input DataFrame with gross_amount and discount_amount
            
        Returns:
            DataFrame with tax_amount column added
        """
        self.logger.info(f"Calculating tax_amount at rate {self.tax_rate}")
        return df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate)
            .cast(DecimalType(16, 2))
        )
    
    def calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate net amount: gross_amount - discount_amount + tax_amount
        
        Args:
            df: Input DataFrame with gross, discount, and tax amounts
            
        Returns:
            DataFrame with net_amount column added
        """
        self.logger.info("Calculating net_amount")
        return df.withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
            .cast(DecimalType(16, 2))
        )
    
    def calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin percentage:
        cost = quantity * unit_price * cost_ratio
        profit_margin = ((net_amount - cost) / net_amount) * 100
        
        Args:
            df: Input DataFrame with net_amount
            
        Returns:
            DataFrame with profit_margin column added
        """
        self.logger.info(f"Calculating profit_margin with cost_ratio {self.cost_ratio}")
        
        # Calculate cost
        df = df.withColumn(
            "cost",
            (F.col("quantity") * F.col("unit_price") * self.cost_ratio)
            .cast(DecimalType(16, 2))
        )
        
        # Calculate profit margin percentage
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                (((F.col("net_amount") - F.col("cost")) / F.col("net_amount")) * 100)
                .cast(DecimalType(5, 2))
            ).otherwise(F.lit(0).cast(DecimalType(5, 2)))
        )
        
        # Drop temporary cost column
        return df.drop("cost")
    
    def categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount:
        - HIGH: >= 2000
        - MEDIUM: >= 500
        - LOW: < 500
        
        Args:
            df: Input DataFrame with gross_amount
            
        Returns:
            DataFrame with category column added
        """
        self.logger.info("Categorizing sales by gross_amount")
        return df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= self.category_high_threshold,
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= self.category_medium_threshold,
                F.lit("MEDIUM")
            ).otherwise(
                F.lit("LOW")
            )
        )
    
    def add_metadata_columns(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add ETL metadata columns.
        
        Args:
            df: Input DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            DataFrame with metadata columns added
        """
        self.logger.info("Adding metadata columns")
        
        # Generate analytics_id: ANL + trans_id + timestamp
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        
        # Add loaded timestamp
        df = df.withColumn("loaded_at", F.current_timestamp())
        
        # Add loaded by (could be parameterized)
        df = df.withColumn("loaded_by", F.lit(self.config.get("etl_user", "SYSTEM")))
        
        return df
    
    def rename_quantity_column(self, df: DataFrame) -> DataFrame:
        """
        Rename quantity to total_quantity to match target schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with renamed column
        """
        return df.withColumnRenamed("quantity", "total_quantity")
    
    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """
        Select and order columns according to analytics schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with final column selection
        """
        return df.select(
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
            "loaded_at",
            "loaded_by"
        )
    
    def transform(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Execute complete transformation pipeline with all business logic.
        
        Pipeline steps:
        1. Calculate gross_amount
        2. Calculate discount_amount (tiered)
        3. Calculate tax_amount
        4. Calculate net_amount
        5. Calculate profit_margin
        6. Categorize sales
        7. Add metadata columns
        8. Rename and select final columns
        
        Args:
            raw_df: Input DataFrame with raw sales data
            etl_run_id: Unique ETL run identifier
            
        Returns:
            Transformed DataFrame ready for loading
        """
        self.logger.info(f"Starting transformation pipeline for ETL run: {etl_run_id}")
        start_time = datetime.now()
        
        # Log input record count
        input_count = raw_df.count()
        self.logger.info(f"Input records: {input_count}")
        
        # Execute transformation pipeline
        df = (raw_df
              .transform(self.calculate_gross_amount)
              .transform(self.calculate_discount_amount)
              .transform(self.calculate_tax_amount)
              .transform(self.calculate_net_amount)
              .transform(self.calculate_profit_margin)
              .transform(self.categorize_sales)
              .transform(lambda d: self.add_metadata_columns(d, etl_run_id))
              .transform(self.rename_quantity_column)
              .transform(self.select_final_columns)
        )
        
        # Cache for multiple operations
        df.cache()
        
        # Log output record count
        output_count = df.count()
        self.logger.info(f"Output records: {output_count}")
        
        # Log transformation statistics
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Transformation completed in {duration:.2f} seconds")
        
        # Log sample statistics
        self._log_statistics(df)
        
        return df
    
    def _log_statistics(self, df: DataFrame) -> None:
        """
        Log transformation statistics for monitoring.
        
        Args:
            df: Transformed DataFrame
        """
        try:
            stats = df.agg(
                F.sum("gross_amount").alias("total_gross"),
                F.sum("discount_amount").alias("total_discount"),
                F.sum("net_amount").alias("total_net"),
                F.avg("profit_margin").alias("avg_margin"),
                F.count("*").alias("record_count")
            ).collect()[0]
            
            self.logger.info("=== Transformation Statistics ===")
            self.logger.info(f"Total Records: {stats['record_count']}")
            self.logger.info(f"Total Gross Amount: ${stats['total_gross']:,.2f}")
            self.logger.info(f"Total Discount: ${stats['total_discount']:,.2f}")
            self.logger.info(f"Total Net Amount: ${stats['total_net']:,.2f}")
            self.logger.info(f"Average Profit Margin: {stats['avg_margin']:.2f}%")
            
            # Category distribution
            category_dist = df.groupBy("category").count().collect()
            self.logger.info("Category Distribution:")
            for row in category_dist:
                self.logger.info(f"  {row['category']}: {row['count']}")
                
        except Exception as e:
            self.logger.warning(f"Could not compute statistics: {str(e)}")


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(app_name: str = "SalesETLTransformation") -> SparkSession:
    """
    Create and configure Spark session.
    
    Args:
        app_name: Application name for Spark UI
        
    Returns:
        Configured SparkSession
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting transformation module test")
    
    # Load configuration
    config = load_config()
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create transformation engine
    transformer = TransformationEngine(spark, config)
    
    # Generate ETL run ID
    etl_run_id = f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Create sample data for testing
    from datetime import date
    sample_data = [
        ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
        ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
        ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
        ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
    ]
    
    # Create DataFrame with schema
    raw_df = spark.createDataFrame(
        sample_data,
        ["trans_id", "trans_date", "customer_id", "product_id", "quantity", 
         "unit_price", "currency", "sales_rep", "region", "status"]
    )
    
    # Execute transformation
    analytics_df = transformer.transform(raw_df, etl_run_id)
    
    # Show results
    logger.info("=== Transformation Results ===")
    analytics_df.show(truncate=False)
    analytics_df.printSchema()
    
    spark.stop()
    logger.info("Transformation test completed")