"""
Data extraction component for ETL pipeline.

Extracts raw sales data from source systems.
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
import pyspark.sql.functions as F

from src.protocols import (
    ETLExtractor,
    ETLLoggerProtocol,
    ETLConfig,
    ExecutionResult,
    ProcessStep,
    StatusCode,
    ExtractError
)


class SalesDataExtractor(ETLExtractor):
    """
    Extracts raw sales data from source.
    
    Implements the ETLExtractor protocol to extract sales transactions
    from Delta tables, JDBC sources, or file systems.
    """

    def __init__(
        self,
        logger: ETLLoggerProtocol,
        config: ETLConfig,
        source_path: Optional[str] = None,
        source_type: str = "delta"
    ):
        """
        Initialize the extractor.

        Args:
            logger: Logger instance
            config: ETL configuration
            source_path: Path to source data
            source_type: Type of source (delta, parquet, jdbc)
        """
        super().__init__(logger, config)
        self._source_path = source_path
        self._source_type = source_type

    def get_component_name(self) -> str:
        """Get the component name."""
        return "SalesDataExtractor"

    def validate_prerequisites(self) -> bool:
        """Validate that prerequisites are met."""
        if not self._source_path:
            self._logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.ERROR.value,
                message="Source path is not configured"
            )
            return False
        return True

    def execute(self, **kwargs) -> ExecutionResult:
        """
        Execute the extraction component.

        Args:
            **kwargs: Must contain 'spark', 'from_date', 'to_date'

        Returns:
            ExecutionResult with extraction statistics
        """
        spark = kwargs.get('spark')
        from_date = kwargs.get('from_date')
        to_date = kwargs.get('to_date')
        
        if not all([spark, from_date, to_date]):
            raise ExtractError(
                "Missing required parameters: spark, from_date, to_date",
                error_step=ProcessStep.EXTRACT.value
            )
        
        start_time = datetime.now()
        
        try:
            df = self.extract_data(spark, from_date, to_date)
            record_count = df.count()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return ExecutionResult(
                success=True,
                records_total=record_count,
                records_success=record_count,
                records_error=0,
                message=f"Extracted {record_count} records",
                duration_seconds=duration
            )
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return ExecutionResult(
                success=False,
                records_total=0,
                records_success=0,
                records_error=0,
                message=f"Extraction failed: {str(e)}",
                duration_seconds=duration
            )

    def extract_data(
        self,
        spark: SparkSession,
        from_date: datetime,
        to_date: datetime
    ) -> DataFrame:
        """
        Extract raw sales data from source.

        Args:
            spark: SparkSession instance
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame containing raw sales records

        Raises:
            ExtractError: If extraction fails
        """
        self._logger.log_message(
            step=ProcessStep.EXTRACT.value,
            status=StatusCode.INFO.value,
            message=f"Starting extraction from {from_date} to {to_date}"
        )
        
        try:
            # Get raw data schema
            schema = self._get_raw_sales_schema()
            
            # Read from source based on type
            if self._source_type == "delta":
                df = self._read_from_delta(spark, schema)
            elif self._source_type == "parquet":
                df = self._read_from_parquet(spark, schema)
            elif self._source_type == "jdbc":
                df = self._read_from_jdbc(spark)
            else:
                raise ExtractError(
                    f"Unsupported source type: {self._source_type}",
                    error_step=ProcessStep.EXTRACT.value
                )
            
            # Filter by date range and status
            df = df.filter(
                (F.col("trans_date") >= F.lit(from_date.date())) &
                (F.col("trans_date") <= F.lit(to_date.date())) &
                (F.col("status") == F.lit(StatusCode.NEW.value))
            )
            
            record_count = df.count()
            
            self._logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.SUCCESS.value,
                message=f"Extracted {record_count} records successfully",
                records_processed=record_count,
                records_success=record_count
            )
            
            return df
            
        except Exception as e:
            self._logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.ERROR.value,
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(
                f"Failed to extract data: {str(e)}",
                error_step=ProcessStep.EXTRACT.value
            ) from e

    def _get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
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

    def _read_from_delta(
        self,
        spark: SparkSession,
        schema: StructType
    ) -> DataFrame:
        """Read data from Delta table."""
        return spark.read.format("delta").load(self._source_path)

    def _read_from_parquet(
        self,
        spark: SparkSession,
        schema: StructType
    ) -> DataFrame:
        """Read data from Parquet files."""
        return spark.read.schema(schema).parquet(self._source_path)

    def _read_from_jdbc(self, spark: SparkSession) -> DataFrame:
        """Read data from JDBC source."""
        # This would read JDBC properties from config
        raise NotImplementedError("JDBC extraction not implemented yet")