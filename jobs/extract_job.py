"""
Spark job for data extraction.
Standalone job executed by SparkSubmitOperator.
"""
import argparse
import yaml
import logging
from pyspark.sql import SparkSession

from src.extract import SalesExtractor


def main():
    """Main extraction job."""
    parser = argparse.ArgumentParser(description='Sales Data Extraction Job')
    parser.add_argument('--from_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to_date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--output_path', default='/tmp/extracted_data', help='Output path')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SalesDataExtraction") \
        .getOrCreate()
    
    try:
        logger.info(f"Starting extraction job for {args.from_date} to {args.to_date}")
        
        # Extract data
        extractor = SalesExtractor(spark, config)
        raw_df = extractor.extract_data(args.from_date, args.to_date)
        
        # Write to intermediate storage
        raw_df.write \
            .mode('overwrite') \
            .parquet(args.output_path)
        
        record_count = raw_df.count()
        logger.info(f"Extraction completed. Records: {record_count}")
        
    except Exception as e:
        logger.error(f"Extraction job failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()