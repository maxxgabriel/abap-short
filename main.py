#!/usr/bin/env python3
"""
Main entry point for the ABAP to PySpark migration project.
Discovers and executes all pipeline modules in the correct order.
"""

import sys
import os
import importlib.util
import logging
from pathlib import Path

# Add src directory to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/pipeline.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


def ensure_directories():
    """Create necessary directories if they don't exist."""
    directories = ['data', 'logs', 'output', 'config']
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    logger.info("Ensured all required directories exist")


def load_config():
    """Load configuration from config.yaml."""
    try:
        import yaml
        config_path = Path('config.yaml')
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info("Configuration loaded successfully")
            return config
        else:
            logger.warning("config.yaml not found, using default configuration")
            return {}
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return {}


def import_module_from_path(module_name, file_path):
    """Dynamically import a module from a file path."""
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            return module
        return None
    except Exception as e:
        logger.error(f"Error importing {module_name} from {file_path}: {e}")
        return None


def run_orchestrator():
    """Run the main orchestrator if it exists."""
    orchestrator_path = src_path / "orchestrator.py"
    if orchestrator_path.exists():
        logger.info("Running orchestrator...")
        orchestrator = import_module_from_path("orchestrator", orchestrator_path)
        if orchestrator and hasattr(orchestrator, 'main'):
            orchestrator.main()
            return True
        elif orchestrator and hasattr(orchestrator, 'run_pipeline'):
            orchestrator.run_pipeline()
            return True
        elif orchestrator and hasattr(orchestrator, 'Orchestrator'):
            orch_instance = orchestrator.Orchestrator()
            if hasattr(orch_instance, 'run'):
                orch_instance.run()
                return True
    return False


def run_main_module():
    """Run the main module if it exists in src."""
    main_path = src_path / "main.py"
    if main_path.exists():
        logger.info("Running main module...")
        main_module = import_module_from_path("src_main", main_path)
        if main_module and hasattr(main_module, 'main'):
            main_module.main()
            return True
    return False


def run_etl_pipeline():
    """Run ETL pipeline modules in order."""
    logger.info("Running ETL pipeline...")
    
    # Import necessary modules
    extract_path = src_path / "extract.py"
    transform_path = src_path / "transform.py"
    load_path = src_path / "load.py"
    
    modules_exist = all([
        extract_path.exists(),
        transform_path.exists(),
        load_path.exists()
    ])
    
    if not modules_exist:
        logger.warning("Not all ETL modules (extract, transform, load) found")
        return False
    
    try:
        # Extract
        logger.info("Step 1: Extract")
        extract_module = import_module_from_path("extract", extract_path)
        
        # Transform
        logger.info("Step 2: Transform")
        transform_module = import_module_from_path("transform", transform_path)
        
        # Load
        logger.info("Step 3: Load")
        load_module = import_module_from_path("load", load_path)
        
        # Execute if modules have main functions
        if extract_module and hasattr(extract_module, 'main'):
            data = extract_module.main()
        
        if transform_module and hasattr(transform_module, 'main'):
            if 'data' in locals():
                data = transform_module.main(data)
            else:
                data = transform_module.main()
        
        if load_module and hasattr(load_module, 'main'):
            if 'data' in locals():
                load_module.main(data)
            else:
                load_module.main()
        
        return True
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {e}")
        return False


def main():
    """Main execution function."""
    try:
        logger.info("="*60)
        logger.info("Starting ABAP to PySpark Migration Pipeline")
        logger.info("="*60)
        
        # Ensure directories exist
        ensure_directories()
        
        # Load configuration
        config = load_config()
        
        # Try to run in order of preference
        # 1. Check for orchestrator
        if run_orchestrator():
            logger.info("Pipeline executed via orchestrator")
        # 2. Check for main module in src
        elif run_main_module():
            logger.info("Pipeline executed via main module")
        # 3. Run ETL pipeline
        elif run_etl_pipeline():
            logger.info("Pipeline executed via ETL modules")
        else:
            logger.warning("No orchestrator, main, or complete ETL modules found")
            logger.info("Listing available modules in src/:")
            for py_file in src_path.glob("*.py"):
                if py_file.name != "__init__.py":
                    logger.info(f"  - {py_file.name}")
        
        logger.info("="*60)
        logger.info("Pipeline execution completed")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()