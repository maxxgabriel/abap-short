"""
Main entry point for Sales ETL pipeline.
"""

from pyspark.sql import SparkSession
import yaml
import argparse
from datetime import datetime, timedelta

from src.orchestrator import ETLOrchestrator


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(config: dict) -> SparkSession:
    """
    Create and configure SparkSession.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(config["spark"]["app_name"])
    
    # Apply Spark configurations
    for key, value in config["spark"]["config"].items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Sales ETL Pipeline")
    
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--from-date",
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        help="Start date for extraction (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--to-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for extraction (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--source-path",
        type=str,
        default=None,
        help="Override source path from config"
    )
    
    parser.add_argument(
        "--target-path",
        type=str,
        default=None,
        help="Override target path from config"
    )
    
    parser.add_argument(
        "--use-sample-data",
        action="store_true",
        help="Use sample data for testing"
    )
    
    return parser.parse_args()


def main():
    """Main execution function."""
    
    # Parse arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    
    try:
        # Create orchestrator
        orchestrator = ETLOrchestrator(spark, config)
        
        # Run ETL pipeline
        success, statistics = orchestrator.run_etl(
            from_date=args.from_date,
            to_date=args.to_date,
            source_path=args.source_path,
            target_path=args.target_path,
            use_sample_data=args.use_sample_data
        )
        
        # Exit with appropriate code
        exit(0 if success else 1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()