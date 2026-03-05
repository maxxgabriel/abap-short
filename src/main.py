"""
Main entry point for Sales ETL Pipeline.
"""
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.orchestrator import ETLOrchestrator


def setup_logging(log_level: str = "INFO"):
    """Configure logging for the application."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("etl_execution.log")
        ]
    )


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sales Data ETL Pipeline"
    )
    
    parser.add_argument(
        "--from-date",
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: 7 days ago)"
    )
    
    parser.add_argument(
        "--to-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


def main():
    """Main execution function."""
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("*" * 70)
        logger.info("Sales Data ETL Process")
        logger.info("*" * 70)
        logger.info(f"Date Range: {args.from_date} to {args.to_date}")
        logger.info(f"Config: {args.config}")
        
        # Create orchestrator and run ETL
        orchestrator = ETLOrchestrator(config_path=args.config)
        results = orchestrator.run(
            from_date=args.from_date,
            to_date=args.to_date
        )
        
        # Display summary
        orchestrator.display_summary(results)
        
        # Cleanup
        orchestrator.stop()
        
        # Exit with appropriate status code
        exit(0 if results["success"] else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()