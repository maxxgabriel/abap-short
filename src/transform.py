"""
Transform module for Sales ETL Pipeline
Applies business rules and calculations to convert raw sales data to analytics format
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, udf, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Transforms raw sales data with business rules"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.business_rules = config['business_rules']
        self.schema = self._get_analytics_schema()
    
    @staticmethod
    def _get_analytics_schema() -> StructType:
        """Define schema for analytics data"""
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=False),
            StructField("loaded_by", StringType(), nullable=False)
        ])
    
    def transform_data(
        self, 
        df_raw: DataFrame, 
        etl_run_id: str
    ) -> DataFrame:
        """
        Apply all transformations to raw sales data
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            logger.info("Starting data transformation")
            
            # Step 1: Calculate gross amount
            df_with_gross = df_raw.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Step 2: Calculate discount using CASE WHEN logic
            df_with_discount = self._apply_discount_rules(df_with_gross)
            
            # Step 3: Calculate tax
            df_with_tax = self._apply_tax_calculation(df_with_discount)
            
            # Step 4: Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                spark_round(
                    col("gross_amount") - col("discount_amount") + col("tax_amount"),
                    2
                )
            )
            
            # Step 5: Calculate profit margin
            df_with_profit = self._calculate_profit_margin(df_with_net)
            
            # Step 6: Categorize sales using UDF
            df_categorized = self._apply_category_logic(df_with_profit)
            
            # Step 7: Generate analytics ID and add metadata
            df_final = self._finalize_analytics_data(
                df_categorized, 
                etl_run_id
            )
            
            record_count = df_final.count()
            logger.info(f"Transformed {record_count} records")
            
            return df_final
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_discount_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply discount rules using CASE WHEN statements
        Business Rule:
        - Quantity > 15: 10% discount
        - Quantity > 10: 5% discount
        - Otherwise: No discount
        """
        tier1_qty = self.business_rules['discount']['tier1_quantity']
        tier2_qty = self.business_rules['discount']['tier2_quantity']
        tier1_rate = Decimal(str(self.business_rules['discount']['tier1_rate']))
        tier2_rate = Decimal(str(self.business_rules['discount']['tier2_rate']))
        
        df_with_discount = df.withColumn(
            "discount_amount",
            spark_round(
                when(col("quantity") > tier2_qty, col("gross_amount") * lit(tier2_rate))
                .when(col("quantity") > tier1_qty, col("gross_amount") * lit(tier1_rate))
                .otherwise(lit(0.0)),
                2
            )
        )
        
        logger.info(f"Applied discount rules: Tier1={tier1_qty} ({tier1_rate}), Tier2={tier2_qty} ({tier2_rate})")
        return df_with_discount
    
    def _apply_tax_calculation(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax on (gross - discount)
        Business Rule: 8% tax rate
        """
        tax_rate = Decimal(str(self.business_rules['tax_rate']))
        
        df_with_tax = df.withColumn(
            "tax_amount",
            spark_round(
                (col("gross_amount") - col("discount_amount")) * lit(tax_rate),
                2
            )
        )
        
        logger.info(f"Applied tax rate: {tax_rate}")
        return df_with_tax
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin percentage
        Business Rule: Assume cost is 60% of unit price
        Formula: ((net - cost) / net) * 100
        """
        cost_ratio = Decimal(str(self.business_rules['cost_ratio']))
        
        df_with_cost = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * lit(cost_ratio), 2)
        )
        
        df_with_profit = df_with_cost.withColumn(
            "profit_margin",
            spark_round(
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100)
                ).otherwise(lit(0.0)),
                2
            )
        ).drop("cost_amount")
        
        return df_with_profit
    
    def _apply_category_logic(self, df: DataFrame) -> DataFrame:
        """
        Apply category classification using UDF and CASE WHEN
        Business Rule:
        - Gross >= 2000: HIGH
        - Gross >= 500: MEDIUM
        - Otherwise: LOW
        """
        high_threshold = Decimal(str(self.business_rules['category']['high_threshold']))
        medium_threshold = Decimal(str(self.business_rules['category']['medium_threshold']))
        
        # Method 1: Using CASE WHEN (preferred for simple logic)
        df_categorized = df.withColumn(
            "category",
            when(col("gross_amount") >= high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        logger.info(f"Applied category thresholds: HIGH>={high_threshold}, MEDIUM>={medium_threshold}")
        return df_categorized
    
    def _register_category_udf(self):
        """
        Register UDF for complex category logic (alternative approach)
        This demonstrates UDF usage for more complex business rules
        """
        high_threshold = Decimal(str(self.business_rules['category']['high_threshold']))
        medium_threshold = Decimal(str(self.business_rules['category']['medium_threshold']))
        
        @udf(StringType())
        def categorize_sale(gross_amount):
            """
            Complex categorization logic as UDF
            Can include additional business rules beyond simple thresholds
            """
            if gross_amount is None:
                return "UNKNOWN"
            
            amount = Decimal(str(gross_amount))
            
            # Additional complex logic can be added here
            # For example: region-specific thresholds, seasonal adjustments, etc.
            if amount >= high_threshold:
                return "HIGH"
            elif amount >= medium_threshold:
                return "MEDIUM"
            else:
                return "LOW"
        
        self.spark.udf.register("categorize_sale", categorize_sale)
        return categorize_sale
    
    def _finalize_analytics_data(
        self, 
        df: DataFrame, 
        etl_run_id: str
    ) -> DataFrame:
        """
        Generate analytics ID and add final metadata
        """
        # Generate unique analytics ID
        df_with_id = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                lit("_"),
                current_timestamp().cast("string").substr(12, 6)
            )
        )
        
        # Add ETL metadata
        df_final = df_with_id.withColumn("etl_run_id", lit(etl_run_id)) \
            .withColumn("loaded_at", current_timestamp()) \
            .withColumn("loaded_by", lit("SPARK_ETL"))
        
        # Rename quantity column for consistency
        df_final = df_final.withColumnRenamed("quantity", "total_quantity")
        
        # Select final columns in correct order
        final_columns = [
            "analytics_id", "trans_date", "customer_id", "product_id",
            "total_quantity", "gross_amount", "net_amount", "discount_amount",
            "tax_amount", "currency", "sales_rep", "region", "profit_margin",
            "category", "etl_run_id", "loaded_at", "loaded_by"
        ]
        
        return df_final.select(*final_columns)