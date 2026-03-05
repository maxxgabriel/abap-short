#!/usr/bin/env python3
"""
Main entry point for ABAP short PySpark pipeline.
Discovers and executes all pipeline modules.
"""
import os
import sys
import importlib
import inspect
from pathlib import Path

def setup_python_path():
    """Add src directory to Python path."""
    project_root = Path(__file__).parent
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

def initialize_spark_session():
    """Initialize Spark session for the pipeline."""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("ABAP_Short_Pipeline") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultSize", "2g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print(f"✓ Spark session initialized: {spark.version}")
        return spark
    except Exception as e:
        print(f"✗ Failed to initialize Spark session: {e}")
        return None

def load_config():
    """Load configuration from config.yaml if it exists."""
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            print(f"✓ Configuration loaded from {config_path}")
            return config
        except Exception as e:
            print(f"⚠ Warning: Could not load config.yaml: {e}")
    return {}

def run_orchestrator(spark, config):
    """Try to run the orchestrator module if it exists."""
    try:
        from src import orchestrator
        
        # Look for main entry point functions
        if hasattr(orchestrator, 'run_pipeline'):
            print("✓ Found orchestrator.run_pipeline()")
            result = orchestrator.run_pipeline(spark=spark, config=config)
            return True, result
        elif hasattr(orchestrator, 'main'):
            print("✓ Found orchestrator.main()")
            result = orchestrator.main(spark=spark)
            return True, result
        elif hasattr(orchestrator, 'orchestrate'):
            print("✓ Found orchestrator.orchestrate()")
            result = orchestrator.orchestrate(spark=spark, config=config)
            return True, result
        elif hasattr(orchestrator, 'Orchestrator'):
            print("✓ Found Orchestrator class")
            orch = orchestrator.Orchestrator(spark=spark, config=config)
            if hasattr(orch, 'run'):
                result = orch.run()
            elif hasattr(orch, 'execute'):
                result = orch.execute()
            else:
                result = None
            return True, result
        else:
            print("⚠ Orchestrator module found but no standard entry point")
            return False, None
    except ImportError:
        print("⚠ No orchestrator module found")
        return False, None
    except Exception as e:
        print(f"✗ Error running orchestrator: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def run_main_module(spark, config):
    """Try to run src/main.py if it exists."""
    try:
        from src import main as main_module
        
        if hasattr(main_module, 'main'):
            print("✓ Found src/main.main()")
            result = main_module.main(spark=spark)
            return True, result
        elif hasattr(main_module, 'run'):
            print("✓ Found src/main.run()")
            result = main_module.run(spark=spark, config=config)
            return True, result
        else:
            print("⚠ src/main.py found but no standard entry point")
            return False, None
    except ImportError:
        print("⚠ No src/main.py module found")
        return False, None
    except Exception as e:
        print(f"✗ Error running src/main: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def run_etl_pipeline(spark, config):
    """Try to run extract -> transform -> load pipeline."""
    try:
        success = True
        results = {}
        
        # Extract
        try:
            from src import extract
            print("✓ Running extract module")
            if hasattr(extract, 'extract_data'):
                results['extract'] = extract.extract_data(spark=spark, config=config)
            elif hasattr(extract, 'run'):
                results['extract'] = extract.run(spark=spark, config=config)
            elif hasattr(extract, 'Extractor'):
                extractor = extract.Extractor(spark=spark, config=config)
                results['extract'] = extractor.extract() if hasattr(extractor, 'extract') else extractor.run()
        except ImportError:
            print("⚠ No extract module found")
            success = False
        
        # Transform
        try:
            from src import transform
            print("✓ Running transform module")
            if hasattr(transform, 'transform_data'):
                results['transform'] = transform.transform_data(spark=spark, config=config, data=results.get('extract'))
            elif hasattr(transform, 'run'):
                results['transform'] = transform.run(spark=spark, config=config, data=results.get('extract'))
            elif hasattr(transform, 'Transformer'):
                transformer = transform.Transformer(spark=spark, config=config)
                results['transform'] = transformer.transform(results.get('extract')) if hasattr(transformer, 'transform') else transformer.run()
        except ImportError:
            print("⚠ No transform module found")
            success = False
        
        # Load
        try:
            from src import load
            print("✓ Running load module")
            if hasattr(load, 'load_data'):
                results['load'] = load.load_data(spark=spark, config=config, data=results.get('transform'))
            elif hasattr(load, 'run'):
                results['load'] = load.run(spark=spark, config=config, data=results.get('transform'))
            elif hasattr(load, 'Loader'):
                loader = load.Loader(spark=spark, config=config)
                results['load'] = loader.load(results.get('transform')) if hasattr(loader, 'load') else loader.run()
        except ImportError:
            print("⚠ No load module found")
            success = False
        
        return success, results
    except Exception as e:
        print(f"✗ Error running ETL pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def main():
    """Main execution function."""
    print("=" * 70)
    print("ABAP Short - PySpark Pipeline")
    print("=" * 70)
    
    # Setup
    setup_python_path()
    spark = initialize_spark_session()
    if not spark:
        print("✗ Cannot proceed without Spark session")
        sys.exit(1)
    
    config = load_config()
    
    # Create output directories
    for directory in ['data', 'output', 'logs']:
        Path(directory).mkdir(exist_ok=True)
    
    print("\n" + "=" * 70)
    print("Starting Pipeline Execution")
    print("=" * 70 + "\n")
    
    # Try different execution strategies
    executed = False
    
    # Strategy 1: Run orchestrator
    print("Strategy 1: Looking for orchestrator...")
    success, result = run_orchestrator(spark, config)
    if success:
        executed = True
        print("✓ Orchestrator executed successfully")
    
    # Strategy 2: Run src/main.py
    if not executed:
        print("\nStrategy 2: Looking for src/main.py...")
        success, result = run_main_module(spark, config)
        if success:
            executed = True
            print("✓ Main module executed successfully")
    
    # Strategy 3: Run ETL pipeline
    if not executed:
        print("\nStrategy 3: Running ETL pipeline...")
        success, result = run_etl_pipeline(spark, config)
        if success:
            executed = True
            print("✓ ETL pipeline executed successfully")
    
    # Cleanup
    if executed:
        print("\n" + "=" * 70)
        print("Pipeline Execution Complete")
        print("=" * 70)
        print(f"\nResults: {result}")
    else:
        print("\n" + "=" * 70)
        print("⚠ Warning: No pipeline execution strategy succeeded")
        print("=" * 70)
        print("\nPlease check that your src/ directory contains one of:")
        print("  - orchestrator.py with run_pipeline(), main(), or orchestrate()")
        print("  - main.py with main() or run()")
        print("  - extract.py, transform.py, and load.py modules")
    
    spark.stop()
    print("\n✓ Spark session stopped")
    
    return 0 if executed else 1

if __name__ == "__main__":
    sys.exit(main())