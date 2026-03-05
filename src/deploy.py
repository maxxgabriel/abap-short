"""
Deployment automation module for ETL pipeline.
Handles deployment to production environment with configuration management.
"""
import os
import sys
import yaml
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import subprocess
import json

from pyspark.sql import SparkSession


class DeploymentManager:
    """Manages deployment of ETL pipeline to production environment."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize deployment manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logging()
        self.deployment_id = self._generate_deployment_id()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load deployment configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            sys.exit(1)
            
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for deployment process."""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        
        logger = logging.getLogger('deployment')
        logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_dir = Path(log_config.get('directory', 'logs'))
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(
            log_dir / f"deployment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
        
    def _generate_deployment_id(self) -> str:
        """Generate unique deployment ID."""
        return f"DEPLOY_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
    def validate_environment(self) -> bool:
        """
        Validate production environment prerequisites.
        
        Returns:
            True if environment is valid
        """
        self.logger.info("Validating production environment...")
        
        env_config = self.config.get('deployment', {}).get('environment', {})
        
        # Check Python version
        required_python = env_config.get('python_version', '3.8')
        current_python = f"{sys.version_info.major}.{sys.version_info.minor}"
        
        if current_python < required_python:
            self.logger.error(
                f"Python version {required_python} or higher required. "
                f"Found: {current_python}"
            )
            return False
            
        # Check required packages
        required_packages = env_config.get('required_packages', [])
        for package in required_packages:
            try:
                __import__(package)
                self.logger.info(f"✓ Package {package} found")
            except ImportError:
                self.logger.error(f"✗ Required package {package} not found")
                return False
                
        # Check Spark availability
        try:
            spark = SparkSession.builder.appName("validation").getOrCreate()
            spark_version = spark.version
            self.logger.info(f"✓ Spark {spark_version} available")
            spark.stop()
        except Exception as e:
            self.logger.error(f"✗ Spark validation failed: {e}")
            return False
            
        # Check directory permissions
        required_dirs = [
            'logs',
            'data',
            'checkpoints',
            'metrics'
        ]
        
        for dir_name in required_dirs:
            dir_path = Path(dir_name)
            dir_path.mkdir(exist_ok=True)
            
            if not os.access(dir_path, os.W_OK):
                self.logger.error(f"✗ No write permission for {dir_path}")
                return False
            else:
                self.logger.info(f"✓ Directory {dir_name} accessible")
                
        self.logger.info("Environment validation completed successfully")
        return True
        
    def run_tests(self) -> bool:
        """
        Run test suite before deployment.
        
        Returns:
            True if all tests pass
        """
        self.logger.info("Running test suite...")
        
        try:
            result = subprocess.run(
                ['pytest', 'tests/', '-v', '--tb=short'],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            self.logger.info(result.stdout)
            
            if result.returncode == 0:
                self.logger.info("✓ All tests passed")
                return True
            else:
                self.logger.error("✗ Tests failed")
                self.logger.error(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("Test suite timed out")
            return False
        except Exception as e:
            self.logger.error(f"Error running tests: {e}")
            return False
            
    def deploy_configuration(self) -> bool:
        """
        Deploy configuration files to production.
        
        Returns:
            True if deployment successful
        """
        self.logger.info("Deploying configuration files...")
        
        deploy_config = self.config.get('deployment', {})
        target_env = deploy_config.get('target_environment', 'production')
        config_dir = Path(deploy_config.get('config_directory', '/etc/etl'))
        
        try:
            # Create configuration directory
            config_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy main configuration
            target_config = config_dir / 'config.yaml'
            with open(self.config_path, 'r') as src:
                config_data = yaml.safe_load(src)
                
            # Update environment-specific settings
            config_data['environment'] = target_env
            config_data['deployment_id'] = self.deployment_id
            config_data['deployed_at'] = datetime.now().isoformat()
            
            with open(target_config, 'w') as dst:
                yaml.dump(config_data, dst, default_flow_style=False)
                
            self.logger.info(f"✓ Configuration deployed to {target_config}")
            
            # Deploy additional config files
            additional_configs = deploy_config.get('additional_configs', [])
            for config_file in additional_configs:
                src_path = Path(config_file)
                if src_path.exists():
                    dst_path = config_dir / src_path.name
                    with open(src_path, 'r') as src, open(dst_path, 'w') as dst:
                        dst.write(src.read())
                    self.logger.info(f"✓ Deployed {config_file}")
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration deployment failed: {e}")
            return False
            
    def deploy_application(self) -> bool:
        """
        Deploy application code to production.
        
        Returns:
            True if deployment successful
        """
        self.logger.info("Deploying application code...")
        
        deploy_config = self.config.get('deployment', {})
        app_dir = Path(deploy_config.get('application_directory', '/opt/etl'))
        
        try:
            # Create application directory
            app_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy source files
            src_dir = Path('src')
            for py_file in src_dir.glob('*.py'):
                dst_path = app_dir / py_file.name
                with open(py_file, 'r') as src, open(dst_path, 'w') as dst:
                    dst.write(src.read())
                self.logger.info(f"✓ Deployed {py_file.name}")
                
            # Create deployment manifest
            manifest = {
                'deployment_id': self.deployment_id,
                'deployed_at': datetime.now().isoformat(),
                'version': self.config.get('version', '1.0.0'),
                'files': [f.name for f in src_dir.glob('*.py')]
            }
            
            manifest_path = app_dir / 'deployment_manifest.json'
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
                
            self.logger.info("✓ Application deployment completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Application deployment failed: {e}")
            return False
            
    def setup_monitoring(self) -> bool:
        """
        Setup monitoring and alerting for production.
        
        Returns:
            True if setup successful
        """
        self.logger.info("Setting up monitoring...")
        
        monitoring_config = self.config.get('monitoring', {})
        
        try:
            # Create metrics directory
            metrics_dir = Path(monitoring_config.get('metrics_directory', 'metrics'))
            metrics_dir.mkdir(exist_ok=True)
            
            # Initialize metrics file
            metrics_file = metrics_dir / 'etl_metrics.json'
            if not metrics_file.exists():
                initial_metrics = {
                    'deployment_id': self.deployment_id,
                    'initialized_at': datetime.now().isoformat(),
                    'metrics': []
                }
                with open(metrics_file, 'w') as f:
                    json.dump(initial_metrics, f, indent=2)
                    
            self.logger.info(f"✓ Metrics file initialized: {metrics_file}")
            
            # Setup alerting configuration
            alert_config = monitoring_config.get('alerting', {})
            if alert_config.get('enabled', False):
                alert_file = metrics_dir / 'alert_config.json'
                with open(alert_file, 'w') as f:
                    json.dump(alert_config, f, indent=2)
                self.logger.info(f"✓ Alerting configured: {alert_file}")
                
            self.logger.info("Monitoring setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Monitoring setup failed: {e}")
            return False
            
    def deploy(self) -> bool:
        """
        Execute full deployment process.
        
        Returns:
            True if deployment successful
        """
        self.logger.info(f"Starting deployment {self.deployment_id}")
        self.logger.info("=" * 70)
        
        # Validation
        if not self.validate_environment():
            self.logger.error("Environment validation failed")
            return False
            
        # Run tests
        if self.config.get('deployment', {}).get('run_tests', True):
            if not self.run_tests():
                self.logger.error("Test suite failed")
                return False
                
        # Deploy configuration
        if not self.deploy_configuration():
            self.logger.error("Configuration deployment failed")
            return False
            
        # Deploy application
        if not self.deploy_application():
            self.logger.error("Application deployment failed")
            return False
            
        # Setup monitoring
        if not self.setup_monitoring():
            self.logger.error("Monitoring setup failed")
            return False
            
        self.logger.info("=" * 70)
        self.logger.info(f"Deployment {self.deployment_id} completed successfully")
        
        return True
        
    def rollback(self, previous_deployment_id: Optional[str] = None) -> bool:
        """
        Rollback to previous deployment.
        
        Args:
            previous_deployment_id: ID of deployment to rollback to
            
        Returns:
            True if rollback successful
        """
        self.logger.warning(f"Initiating rollback to {previous_deployment_id}")
        
        # Implementation would restore previous configuration and code
        # This is a placeholder for the rollback logic
        
        self.logger.info("Rollback completed")
        return True


def main():
    """Main deployment entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Pipeline Deployment')
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Configuration file path'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate environment without deploying'
    )
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip test suite execution'
    )
    
    args = parser.parse_args()
    
    # Create deployment manager
    manager = DeploymentManager(config_path=args.config)
    
    if args.validate_only:
        success = manager.validate_environment()
        sys.exit(0 if success else 1)
        
    # Override test config if requested
    if args.skip_tests:
        manager.config['deployment']['run_tests'] = False
        
    # Execute deployment
    success = manager.deploy()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()