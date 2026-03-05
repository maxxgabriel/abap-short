"""
Deployment automation module for Sales ETL PySpark jobs.
Handles deployment to production environment with validation and rollback.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import yaml
import subprocess
import requests
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DeploymentConfig:
    """Deployment configuration parameters."""
    environment: str
    spark_master: str
    deploy_mode: str
    app_name: str
    main_script: str
    dependencies: List[str]
    config_files: List[str]
    num_executors: int
    executor_memory: str
    executor_cores: int
    driver_memory: str
    spark_config: Dict[str, str]
    monitoring_endpoint: str
    rollback_enabled: bool
    version: str


class DeploymentValidator:
    """Validates deployment prerequisites and environment."""
    
    def __init__(self, config: DeploymentConfig):
        self.config = config
        
    def validate_environment(self) -> bool:
        """Validate deployment environment."""
        logger.info("Validating deployment environment...")
        
        # Check Spark availability
        if not self._check_spark_available():
            logger.error("Spark is not available")
            return False
            
        # Check required files
        if not self._check_required_files():
            logger.error("Required files are missing")
            return False
            
        # Check configuration validity
        if not self._validate_config():
            logger.error("Configuration validation failed")
            return False
            
        # Check connectivity to monitoring endpoint
        if not self._check_monitoring_connectivity():
            logger.warning("Monitoring endpoint not reachable")
            
        logger.info("Environment validation successful")
        return True
        
    def _check_spark_available(self) -> bool:
        """Check if Spark is available."""
        try:
            result = subprocess.run(
                ['spark-submit', '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Spark availability check failed: {e}")
            return False
            
    def _check_required_files(self) -> bool:
        """Check if all required files exist."""
        files_to_check = [
            self.config.main_script,
            *self.config.config_files,
            *self.config.dependencies
        ]
        
        for file_path in files_to_check:
            if not Path(file_path).exists():
                logger.error(f"Required file not found: {file_path}")
                return False
                
        return True
        
    def _validate_config(self) -> bool:
        """Validate configuration parameters."""
        if self.config.num_executors < 1:
            logger.error("Number of executors must be at least 1")
            return False
            
        if self.config.executor_cores < 1:
            logger.error("Executor cores must be at least 1")
            return False
            
        return True
        
    def _check_monitoring_connectivity(self) -> bool:
        """Check connectivity to monitoring endpoint."""
        try:
            response = requests.get(
                f"{self.config.monitoring_endpoint}/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Monitoring connectivity check failed: {e}")
            return False


class SparkJobDeployer:
    """Deploys PySpark jobs to production environment."""
    
    def __init__(self, config: DeploymentConfig):
        self.config = config
        self.validator = DeploymentValidator(config)
        self.deployment_id = self._generate_deployment_id()
        
    def deploy(self) -> bool:
        """Execute deployment process."""
        logger.info(f"Starting deployment {self.deployment_id}")
        logger.info(f"Environment: {self.config.environment}")
        logger.info(f"Version: {self.config.version}")
        
        try:
            # Validate environment
            if not self.validator.validate_environment():
                raise Exception("Environment validation failed")
                
            # Create backup for rollback
            if self.config.rollback_enabled:
                self._create_backup()
                
            # Deploy configuration files
            self._deploy_config_files()
            
            # Deploy application code
            self._deploy_application()
            
            # Submit Spark job
            job_id = self._submit_spark_job()
            
            # Register with monitoring
            self._register_monitoring(job_id)
            
            logger.info(f"Deployment {self.deployment_id} completed successfully")
            logger.info(f"Spark job ID: {job_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            if self.config.rollback_enabled:
                self._rollback()
            return False
            
    def _generate_deployment_id(self) -> str:
        """Generate unique deployment ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"DEPLOY_{self.config.environment}_{self.config.version}_{timestamp}"
        
    def _create_backup(self):
        """Create backup of current deployment."""
        logger.info("Creating backup for rollback...")
        backup_dir = Path(f"backups/{self.deployment_id}")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup current scripts and configs
        for file_path in [self.config.main_script, *self.config.config_files]:
            if Path(file_path).exists():
                subprocess.run(['cp', file_path, str(backup_dir)])
                
    def _deploy_config_files(self):
        """Deploy configuration files to production."""
        logger.info("Deploying configuration files...")
        
        for config_file in self.config.config_files:
            dest_path = f"/opt/spark/conf/{Path(config_file).name}"
            logger.info(f"Deploying {config_file} to {dest_path}")
            subprocess.run(['cp', config_file, dest_path], check=True)
            
    def _deploy_application(self):
        """Deploy application code to production."""
        logger.info("Deploying application code...")
        
        app_dir = Path("/opt/spark/apps/sales_etl")
        app_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy main script
        subprocess.run(
            ['cp', self.config.main_script, str(app_dir)],
            check=True
        )
        
        # Copy dependencies
        for dep in self.config.dependencies:
            subprocess.run(['cp', dep, str(app_dir)], check=True)
            
    def _submit_spark_job(self) -> str:
        """Submit Spark job to cluster."""
        logger.info("Submitting Spark job...")
        
        spark_submit_cmd = [
            'spark-submit',
            '--master', self.config.spark_master,
            '--deploy-mode', self.config.deploy_mode,
            '--name', f"{self.config.app_name}_{self.deployment_id}",
            '--num-executors', str(self.config.num_executors),
            '--executor-memory', self.config.executor_memory,
            '--executor-cores', str(self.config.executor_cores),
            '--driver-memory', self.config.driver_memory,
        ]
        
        # Add Spark configuration
        for key, value in self.config.spark_config.items():
            spark_submit_cmd.extend(['--conf', f'{key}={value}'])
            
        # Add main script
        spark_submit_cmd.append(self.config.main_script)
        
        logger.info(f"Command: {' '.join(spark_submit_cmd)}")
        
        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Spark submit failed: {result.stderr}")
            
        # Extract job ID from output
        job_id = self._extract_job_id(result.stdout)
        return job_id
        
    def _extract_job_id(self, output: str) -> str:
        """Extract Spark job ID from submit output."""
        for line in output.split('\n'):
            if 'application_' in line.lower():
                # Extract application ID
                parts = line.split()
                for part in parts:
                    if part.startswith('application_'):
                        return part
        return self.deployment_id
        
    def _register_monitoring(self, job_id: str):
        """Register job with monitoring system."""
        logger.info("Registering with monitoring system...")
        
        try:
            response = requests.post(
                f"{self.config.monitoring_endpoint}/jobs/register",
                json={
                    'job_id': job_id,
                    'deployment_id': self.deployment_id,
                    'environment': self.config.environment,
                    'version': self.config.version,
                    'app_name': self.config.app_name,
                    'deployed_at': datetime.now().isoformat()
                },
                timeout=10
            )
            response.raise_for_status()
            logger.info("Monitoring registration successful")
        except Exception as e:
            logger.warning(f"Failed to register with monitoring: {e}")
            
    def _rollback(self):
        """Rollback to previous deployment."""
        logger.warning("Initiating rollback...")
        
        backup_dir = Path(f"backups/{self.deployment_id}")
        if not backup_dir.exists():
            logger.error("Backup directory not found, cannot rollback")
            return
            
        # Restore files from backup
        for backup_file in backup_dir.iterdir():
            dest_path = f"/opt/spark/apps/sales_etl/{backup_file.name}"
            subprocess.run(['cp', str(backup_file), dest_path])
            
        logger.info("Rollback completed")


class DeploymentOrchestrator:
    """Orchestrates the complete deployment process."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> DeploymentConfig:
        """Load deployment configuration from file."""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            
        return DeploymentConfig(
            environment=config_data['environment'],
            spark_master=config_data['spark']['master'],
            deploy_mode=config_data['spark']['deploy_mode'],
            app_name=config_data['spark']['app_name'],
            main_script=config_data['deployment']['main_script'],
            dependencies=config_data['deployment']['dependencies'],
            config_files=config_data['deployment']['config_files'],
            num_executors=config_data['spark']['num_executors'],
            executor_memory=config_data['spark']['executor_memory'],
            executor_cores=config_data['spark']['executor_cores'],
            driver_memory=config_data['spark']['driver_memory'],
            spark_config=config_data['spark']['config'],
            monitoring_endpoint=config_data['monitoring']['endpoint'],
            rollback_enabled=config_data['deployment']['rollback_enabled'],
            version=config_data['deployment']['version']
        )
        
    def execute_deployment(self) -> bool:
        """Execute the deployment process."""
        logger.info("=" * 80)
        logger.info("Sales ETL Production Deployment")
        logger.info("=" * 80)
        
        deployer = SparkJobDeployer(self.config)
        success = deployer.deploy()
        
        if success:
            logger.info("=" * 80)
            logger.info("DEPLOYMENT SUCCESSFUL")
            logger.info("=" * 80)
            self._send_notification("success")
        else:
            logger.error("=" * 80)
            logger.error("DEPLOYMENT FAILED")
            logger.error("=" * 80)
            self._send_notification("failure")
            
        return success
        
    def _send_notification(self, status: str):
        """Send deployment notification."""
        try:
            requests.post(
                f"{self.config.monitoring_endpoint}/notifications",
                json={
                    'deployment_id': datetime.now().strftime("%Y%m%d_%H%M%S"),
                    'environment': self.config.environment,
                    'version': self.config.version,
                    'status': status,
                    'timestamp': datetime.now().isoformat()
                },
                timeout=5
            )
        except Exception as e:
            logger.warning(f"Failed to send notification: {e}")


def main():
    """Main entry point for deployment script."""
    if len(sys.argv) < 2:
        print("Usage: python deploy.py <config_file>")
        sys.exit(1)
        
    config_file = sys.argv[1]
    
    if not Path(config_file).exists():
        print(f"Configuration file not found: {config_file}")
        sys.exit(1)
        
    orchestrator = DeploymentOrchestrator(config_file)
    success = orchestrator.execute_deployment()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()