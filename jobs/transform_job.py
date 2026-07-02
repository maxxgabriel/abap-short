"""
Spark job for data transformation.
Standalone job executed by SparkSubmitOperator.
"""
import argparse
import yaml
import logging
from pyspark.sql import SparkSession

from src.transform import SalesTransformer


def main():
    """Main transformation job."""
    parser = argparse.ArgumentParser(description='Sales Data Transformation Job')
    parser.add_argument('--etl_run_id', required=True, help='ETL run identifier')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--input_path', required=True, help='Input data path')
    parser.add_argument('--output_path', default='/tmp/transformed_data', help='Output path')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SalesDataTransformation") \
        .getOrCreate()
    
    try:
        logger.info(f"Starting transformation job with ETL Run ID: {args.etl_run_id}")
        
        # Read extracted data
        raw_df = spark.read.parquet(args.input_path)
        
        # Transform data
        transformer = SalesTransformer(config, args.etl_run_id)
        analytics_df = transformer.transform_data(raw_df)
        
        # Write transformed data
        analytics_df.write \
            .mode('overwrite') \
            .parquet(args.output_path)
        
        record_count = analytics_df.count()
        logger.info(f"Transformation completed. Records: {record_count}")
        
    except Exception as e:
        logger.error(f"Transformation job failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()