"""
Deployment automation module for ETL pipeline.
Handles configuration validation, environment setup, and rollout orchestration.
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import subprocess
import shutil

from pyspark.sql import SparkSession


class DeploymentError(Exception):
    """Custom exception for deployment failures"""
    pass


class ETLDeployment:
    """Orchestrates ETL deployment and rollout to production"""
    
    def __init__(self, config_path: str, environment: str):
        """
        Initialize deployment manager
        
        Args:
            config_path: Path to deployment configuration file
            environment: Target environment (dev/staging/prod)
        """
        self.config_path = config_path
        self.environment = environment
        self.config = self._load_config()
        self.logger = self._setup_logging()
        self.deployment_id = self._generate_deployment_id()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load deployment configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config.get('deployment', {}).get(self.environment, {})
        except FileNotFoundError:
            raise DeploymentError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise DeploymentError(f"Invalid YAML configuration: {str(e)}")
    
    def _setup_logging(self) -> logging.Logger:
        """Configure deployment logging"""
        log_dir = Path('logs/deployment')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger(f'etl_deployment_{self.environment}')
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = log_dir / f"deployment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def _generate_deployment_id(self) -> str:
        """Generate unique deployment identifier"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"DEPLOY_{self.environment.upper()}_{timestamp}"
    
    def validate_configuration(self) -> bool:
        """Validate deployment configuration"""
        self.logger.info(f"Validating configuration for {self.environment}")
        
        required_keys = ['spark_config', 'paths', 'validation']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            self.logger.error(f"Missing required configuration keys: {missing_keys}")
            return False
        
        # Validate paths exist
        paths = self.config.get('paths', {})
        for path_key, path_value in paths.items():
            path = Path(path_value)
            if not path.exists():
                self.logger.warning(f"Path does not exist: {path_key}={path_value}")
        
        self.logger.info("Configuration validation passed")
        return True
    
    def run_pre_deployment_tests(self) -> bool:
        """Execute pre-deployment validation tests"""
        self.logger.info("Running pre-deployment tests")
        
        try:
            # Run pytest with coverage
            result = subprocess.run(
                ['pytest', 'tests/', '-v', '--cov=src', '--cov-report=term-missing'],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                self.logger.info("All tests passed successfully")
                self.logger.info(result.stdout)
                return True
            else:
                self.logger.error("Tests failed")
                self.logger.error(result.stdout)
                self.logger.error(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("Tests timed out after 5 minutes")
            return False
        except FileNotFoundError:
            self.logger.error("pytest not found. Install with: pip install pytest pytest-cov")
            return False
    
    def backup_existing_deployment(self) -> Optional[str]:
        """Backup current production deployment"""
        self.logger.info("Creating backup of existing deployment")
        
        backup_dir = Path(self.config['paths'].get('backup_dir', 'backups'))
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f"backup_{self.deployment_id}_{timestamp}"
        
        try:
            # Backup source code
            src_path = Path('src')
            if src_path.exists():
                shutil.copytree(src_path, backup_path / 'src')
            
            # Backup config
            config_path = Path('config.yaml')
            if config_path.exists():
                shutil.copy(config_path, backup_path / 'config.yaml')
            
            self.logger.info(f"Backup created at: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            return None
    
    def deploy_artifacts(self) -> bool:
        """Deploy ETL artifacts to target environment"""
        self.logger.info(f"Deploying artifacts to {self.environment}")
        
        try:
            target_path = Path(self.config['paths']['deployment_dir'])
            target_path.mkdir(parents=True, exist_ok=True)
            
            # Copy source files
            src_files = ['extract.py', 'transform.py', 'load.py', 'monitor.py']
            for src_file in src_files:
                src_path = Path('src') / src_file
                if src_path.exists():
                    shutil.copy(src_path, target_path / src_file)
                    self.logger.info(f"Deployed: {src_file}")
            
            # Copy configuration
            config_src = Path('config.yaml')
            config_dst = target_path / 'config.yaml'
            shutil.copy(config_src, config_dst)
            self.logger.info("Deployed: config.yaml")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            return False
    
    def validate_deployment(self) -> bool:
        """Validate deployed artifacts"""
        self.logger.info("Validating deployment")
        
        target_path = Path(self.config['paths']['deployment_dir'])
        
        # Check required files exist
        required_files = [
            'extract.py', 'transform.py', 'load.py', 
            'monitor.py', 'config.yaml'
        ]
        
        for filename in required_files:
            filepath = target_path / filename
            if not filepath.exists():
                self.logger.error(f"Missing required file: {filename}")
                return False
        
        self.logger.info("Deployment validation passed")
        return True
    
    def run_smoke_tests(self) -> bool:
        """Execute smoke tests on deployed environment"""
        self.logger.info("Running smoke tests")
        
        try:
            # Initialize Spark session with deployed config
            spark = SparkSession.builder \
                .appName(f"ETL_SmokeTest_{self.deployment_id}") \
                .config("spark.sql.shuffle.partitions", "2") \
                .getOrCreate()
            
            # Test basic Spark functionality
            test_data = [(1, "test1"), (2, "test2")]
            df = spark.createDataFrame(test_data, ["id", "value"])
            
            if df.count() != 2:
                self.logger.error("Smoke test failed: DataFrame count mismatch")
                return False
            
            spark.stop()
            self.logger.info("Smoke tests passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Smoke tests failed: {str(e)}")
            return False
    
    def rollback_deployment(self, backup_path: str) -> bool:
        """Rollback to previous deployment"""
        self.logger.warning(f"Rolling back deployment from: {backup_path}")
        
        try:
            target_path = Path(self.config['paths']['deployment_dir'])
            backup_src = Path(backup_path)
            
            # Remove current deployment
            if target_path.exists():
                shutil.rmtree(target_path)
            
            # Restore from backup
            shutil.copytree(backup_src / 'src', target_path)
            shutil.copy(backup_src / 'config.yaml', target_path / 'config.yaml')
            
            self.logger.info("Rollback completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Rollback failed: {str(e)}")
            return False
    
    def execute_deployment(self) -> bool:
        """Execute full deployment pipeline"""
        self.logger.info(f"Starting deployment {self.deployment_id}")
        self.logger.info(f"Target environment: {self.environment}")
        
        backup_path = None
        
        try:
            # Step 1: Validate configuration
            if not self.validate_configuration():
                raise DeploymentError("Configuration validation failed")
            
            # Step 2: Run pre-deployment tests
            if not self.run_pre_deployment_tests():
                raise DeploymentError("Pre-deployment tests failed")
            
            # Step 3: Backup existing deployment
            if self.environment == 'prod':
                backup_path = self.backup_existing_deployment()
                if not backup_path:
                    self.logger.warning("Backup failed, but continuing deployment")
            
            # Step 4: Deploy artifacts
            if not self.deploy_artifacts():
                raise DeploymentError("Artifact deployment failed")
            
            # Step 5: Validate deployment
            if not self.validate_deployment():
                raise DeploymentError("Deployment validation failed")
            
            # Step 6: Run smoke tests
            if not self.run_smoke_tests():
                raise DeploymentError("Smoke tests failed")
            
            self.logger.info(f"Deployment {self.deployment_id} completed successfully")
            return True
            
        except DeploymentError as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            
            # Attempt rollback for production
            if self.environment == 'prod' and backup_path:
                self.logger.info("Attempting automatic rollback")
                if self.rollback_deployment(backup_path):
                    self.logger.info("Rollback successful")
                else:
                    self.logger.error("Rollback failed - manual intervention required")
            
            return False
        
        except Exception as e:
            self.logger.error(f"Unexpected error during deployment: {str(e)}")
            return False


def main():
    """Main deployment entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Deployment Automation')
    parser.add_argument(
        '--environment',
        choices=['dev', 'staging', 'prod'],
        required=True,
        help='Target deployment environment'
    )
    parser.add_argument(
        '--config',
        default='deployment_config.yaml',
        help='Path to deployment configuration file'
    )
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip pre-deployment tests (not recommended for production)'
    )
    
    args = parser.parse_args()
    
    # Confirm production deployment
    if args.environment == 'prod':
        response = input("⚠️  Deploy to PRODUCTION? Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            print("Deployment cancelled")
            sys.exit(0)
    
    # Execute deployment
    deployer = ETLDeployment(args.config, args.environment)
    
    if args.skip_tests:
        deployer.logger.warning("Skipping pre-deployment tests")
        # Override test method
        deployer.run_pre_deployment_tests = lambda: True
    
    success = deployer.execute_deployment()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()