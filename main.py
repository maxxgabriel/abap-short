#!/usr/bin/env python3
"""
Main entry point for the ABAP to PySpark ETL pipeline.
This script discovers and executes all pipeline modules.
"""

import sys
import os
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging():
    """Configure basic logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )
    return logging.getLogger(__name__)

def run_pipeline():
    """Execute the ETL pipeline"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("Starting ABAP to PySpark ETL Pipeline")
    logger.info("=" * 80)
    
    try:
        # Import and run main orchestrator
        logger.info("Importing orchestrator module...")
        from orchestrator import ETLOrchestrator
        
        logger.info("Initializing ETL Orchestrator...")
        orchestrator = ETLOrchestrator()
        
        logger.info("Running ETL pipeline...")
        orchestrator.run()
        
        logger.info("=" * 80)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 80)
        return 0
        
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        logger.info("Attempting alternative entry points...")
        
        try:
            # Try main.py if it exists
            if os.path.exists("src/main.py"):
                logger.info("Executing src/main.py...")
                import main as pipeline_main
                if hasattr(pipeline_main, 'main'):
                    pipeline_main.main()
                elif hasattr(pipeline_main, 'run'):
                    pipeline_main.run()
                else:
                    logger.error("No main() or run() function found in main.py")
                    return 1
                return 0
            
            # Try CLI entry point
            if os.path.exists("src/cli_entry_point_and_airflow_trigger.py"):
                logger.info("Executing CLI entry point...")
                import cli_entry_point_and_airflow_trigger as cli
                if hasattr(cli, 'main'):
                    cli.main()
                    return 0
            
            # Manual orchestration if orchestrator class exists differently
            logger.info("Attempting manual ETL execution...")
            from extract import extract_data
            from transform import transform_data
            from load import load_data
            
            logger.info("Step 1: Extracting data...")
            extracted_data = extract_data()
            
            logger.info("Step 2: Transforming data...")
            transformed_data = transform_data(extracted_data)
            
            logger.info("Step 3: Loading data...")
            load_data(transformed_data)
            
            logger.info("Manual ETL execution completed successfully!")
            return 0
            
        except Exception as inner_e:
            logger.error(f"All execution attempts failed: {inner_e}")
            logger.exception("Detailed traceback:")
            return 1
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        logger.exception("Detailed traceback:")
        return 1

def show_environment_info():
    """Display environment information"""
    logger = logging.getLogger(__name__)
    logger.info("Environment Information:")
    logger.info(f"Python Version: {sys.version}")
    logger.info(f"Working Directory: {os.getcwd()}")
    logger.info(f"Python Path: {sys.path[:3]}")
    
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        logger.info(f"PySpark Version: {SparkContext.version}")
        
        # Check if Spark session can be created
        spark = SparkSession.builder.appName("ABAPPipeline").getOrCreate()
        logger.info(f"Spark Master: {spark.sparkContext.master}")
        logger.info(f"Spark App Name: {spark.sparkContext.appName}")
    except Exception as e:
        logger.warning(f"Could not retrieve Spark info: {e}")

if __name__ == "__main__":
    logger = setup_logging()
    show_environment_info()
    exit_code = run_pipeline()
    sys.exit(exit_code)