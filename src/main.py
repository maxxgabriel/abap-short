"""
Main CLI entry point for Sales ETL Process.

This module provides a command-line interface to orchestrate the ETL pipeline
for sales data processing, including extraction, transformation, and loading phases.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import SparkSession

from src.orchestrator import ETLOrchestrator
from src.logger import ETLLogger
from src.constants import ETLConstants


def setup_logging(config: dict) -> logging.Logger:
    """
    Configure logging framework for the ETL process.

    Args:
        config: Configuration dictionary with logging settings

    Returns:
        Configured logger instance
    """
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_format = log_config.get(
        'format',
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_config.get('file', 'etl_process.log'))
        ]
    )

    return logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the ETL process.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Sales Data ETL Process',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Date range parameters
    default_from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    default_to_date = datetime.now().strftime('%Y-%m-%d')

    parser.add_argument(
        '--from-date',
        type=str,
        default=default_from_date,
        help='Start date for data extraction (YYYY-MM-DD format)'
    )

    parser.add_argument(
        '--to-date',
        type=str,
        default=default_to_date,
        help='End date for data extraction (YYYY-MM-DD format)'
    )

    # Processing mode
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=False,
        help='Run in test mode without committing changes'
    )

    # Configuration
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file'
    )

    # Spark configuration
    parser.add_argument(
        '--master',
        type=str,
        default='local[*]',
        help='Spark master URL'
    )

    parser.add_argument(
        '--app-name',
        type=str,
        default='SalesETL',
        help='Spark application name'
    )

    # Advanced options
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Override batch size from config'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def validate_date_range(from_date_str: str, to_date_str: str) -> tuple:
    """
    Validate and parse date range parameters.

    Args:
        from_date_str: Start date string
        to_date_str: End date string

    Returns:
        Tuple of (from_date, to_date) as datetime objects

    Raises:
        ValueError: If dates are invalid or range is incorrect
    """
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

    if from_date > to_date:
        raise ValueError("From date cannot be later than to date")

    if to_date > datetime.now():
        raise ValueError("To date cannot be in the future")

    return from_date, to_date


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    return config


def create_spark_session(args: argparse.Namespace, config: dict) -> SparkSession:
    """
    Create and configure Spark session.

    Args:
        args: Command-line arguments
        config: Configuration dictionary

    Returns:
        Configured SparkSession
    """
    spark_config = config.get('spark', {})

    builder = SparkSession.builder \
        .appName(args.app_name) \
        .master(args.master)

    # Apply configuration from file
    for key, value in spark_config.items():
        if key != 'master' and key != 'app_name':
            builder = builder.config(f"spark.{key}", value)

    spark = builder.getOrCreate()

    # Set log level
    if not args.verbose:
        spark.sparkContext.setLogLevel("WARN")

    return spark


def print_header(logger: logging.Logger):
    """Print process header."""
    header = """
    ******************************************************************
    *                                                                *
    *              Sales Data ETL Process                            *
    *                                                                *
    ******************************************************************
    """
    logger.info(header)


def print_parameters(logger: logging.Logger, args: argparse.Namespace,
                     from_date: datetime, to_date: datetime):
    """
    Print processing parameters.

    Args:
        logger: Logger instance
        args: Command-line arguments
        from_date: Start date
        to_date: End date
    """
    logger.info("=" * 70)
    logger.info("ETL Process Parameters:")
    logger.info("-" * 70)
    logger.info(f"  Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    logger.info(f"  Test Mode: {'Yes' if args.test_mode else 'No'}")
    logger.info(f"  Config File: {args.config}")
    logger.info(f"  Spark Master: {args.master}")
    logger.info(f"  Batch Size: {args.batch_size if args.batch_size else 'From Config'}")
    logger.info("=" * 70)


def print_summary(logger: logging.Logger, orchestrator: ETLOrchestrator,
                  success: bool, test_mode: bool):
    """
    Print ETL process summary.

    Args:
        logger: Logger instance
        orchestrator: ETL orchestrator instance
        success: Whether the process succeeded
        test_mode: Whether running in test mode
    """
    logger.info("")
    logger.info("=" * 70)

    if success:
        logger.info("*** ETL Process Completed Successfully ***")
        logger.info("")
        orchestrator.display_summary()

        if test_mode:
            logger.info("")
            logger.info("Test mode - No data committed to database")
        else:
            logger.info("")
            logger.info("Data committed to database")
    else:
        logger.error("*** ETL Process Failed ***")
        logger.error("Please check the error logs for details")

    logger.info("=" * 70)


def main() -> int:
    """
    Main entry point for the ETL process.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger = None

    try:
        # Parse arguments
        args = parse_arguments()

        # Load configuration
        config = load_config(args.config)

        # Setup logging
        logger = setup_logging(config)

        # Override config with command-line arguments
        if args.batch_size:
            config['etl']['batch_size'] = args.batch_size

        # Print header
        print_header(logger)

        # Validate dates
        from_date, to_date = validate_date_range(args.from_date, args.to_date)

        # Print parameters
        print_parameters(logger, args, from_date, to_date)

        # Create Spark session
        logger.info("Initializing Spark session...")
        spark = create_spark_session(args, config)
        logger.info(f"Spark session created: {spark.sparkContext.applicationId}")

        # Create orchestrator
        logger.info("Initializing ETL orchestrator...")
        orchestrator = ETLOrchestrator(spark, config, logger)
        logger.info(f"ETL Run ID: {orchestrator.get_etl_run_id()}")
        logger.info("")

        # Run ETL process
        success = orchestrator.run_etl(
            from_date=from_date,
            to_date=to_date,
            test_mode=args.test_mode
        )

        # Print summary
        print_summary(logger, orchestrator, success, args.test_mode)

        # Cleanup
        spark.stop()
        logger.info("Spark session stopped")

        return 0 if success else 1

    except ValueError as ve:
        if logger:
            logger.error(f"Validation error: {ve}")
        else:
            print(f"ERROR: {ve}", file=sys.stderr)
        return 1

    except FileNotFoundError as fe:
        if logger:
            logger.error(f"File not found: {fe}")
        else:
            print(f"ERROR: {fe}", file=sys.stderr)
        return 1

    except Exception as e:
        if logger:
            logger.exception(f"Fatal error: {e}")
        else:
            print(f"FATAL ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())