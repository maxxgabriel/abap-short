#!/usr/bin/env python3
"""
Main entry point for the ABAP to PySpark migrated project.
Automatically discovers and runs all pipeline modules.
"""

import sys
import os
import logging
from pathlib import Path
from typing import List, Optional

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging():
    """Configure basic logging for the main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/pipeline.log', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def ensure_directories():
    """Create necessary directories if they don't exist."""
    dirs = ['data', 'logs', 'checkpoints', 'output']
    for dir_name in dirs:
        Path(dir_name).mkdir(exist_ok=True)

def run_orchestrator(logger):
    """Run the orchestrator if it exists."""
    try:
        from src.orchestrator import Orchestrator
        logger.info("Running pipeline through Orchestrator...")
        orchestrator = Orchestrator()
        orchestrator.run()
        logger.info("Orchestrator completed successfully")
        return True
    except (ImportError, AttributeError) as e:
        logger.debug(f"Orchestrator not available or not runnable: {e}")
        return False

def run_main_module(logger):
    """Run src/main.py if it has a main function or run method."""
    try:
        from src import main as main_module
        logger.info("Running src/main.py module...")
        
        if hasattr(main_module, 'main'):
            main_module.main()
            logger.info("src/main.main() completed successfully")
            return True
        elif hasattr(main_module, 'run'):
            main_module.run()
            logger.info("src/main.run() completed successfully")
            return True
        else:
            logger.debug("src/main.py has no main() or run() function")
            return False
    except ImportError as e:
        logger.debug(f"src/main.py module not found or not runnable: {e}")
        return False

def run_etl_pipeline(logger):
    """Run the ETL pipeline (extract, transform, load)."""
    try:
        from src.extract import extract
        from src.transform import transform
        from src.load import load
        
        logger.info("Running ETL pipeline...")
        
        # Extract
        logger.info("Step 1: Extracting data...")
        extracted_data = extract()
        
        # Transform
        logger.info("Step 2: Transforming data...")
        transformed_data = transform(extracted_data)
        
        # Load
        logger.info("Step 3: Loading data...")
        load(transformed_data)
        
        logger.info("ETL pipeline completed successfully")
        return True
    except Exception as e:
        logger.debug(f"ETL pipeline not runnable: {e}")
        return False

def run_deploy_monitor(logger):
    """Run deployment and monitoring if available."""
    try:
        from src.deploy import deploy
        from src.monitor import monitor
        
        logger.info("Running deployment...")
        deploy()
        
        logger.info("Running monitoring...")
        monitor()
        
        logger.info("Deploy and monitor completed successfully")
        return True
    except Exception as e:
        logger.debug(f"Deploy/Monitor not available: {e}")
        return False

def main():
    """Main execution function."""
    # Setup
    ensure_directories()
    logger = setup_logging()
    
    logger.info("="*60)
    logger.info("ABAP to PySpark Migration - Pipeline Execution")
    logger.info("="*60)
    
    # Try different execution strategies in order of preference
    execution_strategies = [
        ("Orchestrator", run_orchestrator),
        ("Main Module", run_main_module),
        ("ETL Pipeline", run_etl_pipeline),
        ("Deploy/Monitor", run_deploy_monitor)
    ]
    
    success = False
    for strategy_name, strategy_func in execution_strategies:
        logger.info(f"\nAttempting to run: {strategy_name}")
        try:
            if strategy_func(logger):
                success = True
                break
        except Exception as e:
            logger.error(f"Error running {strategy_name}: {str(e)}", exc_info=True)
            continue
    
    if not success:
        logger.warning(
            "\nNo runnable pipeline entry point found. "
            "This may be expected if the project requires manual configuration."
        )
        logger.info("\nAvailable modules in src/:")
        src_path = Path(__file__).parent / "src"
        if src_path.exists():
            for py_file in sorted(src_path.glob("*.py")):
                if py_file.name != "__init__.py":
                    logger.info(f"  - {py_file.name}")
        
        logger.info("\nTo run the pipeline, please:")
        logger.info("1. Review the config.yaml configuration")
        logger.info("2. Ensure data sources are properly configured")
        logger.info("3. Run specific modules as needed")
        sys.exit(1)
    
    logger.info("\n" + "="*60)
    logger.info("Pipeline execution completed successfully!")
    logger.info("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)