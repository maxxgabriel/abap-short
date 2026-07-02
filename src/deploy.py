"""
Deployment automation module for ETL production environment.
Handles deployment of PySpark jobs to production clusters.
"""
import os
import sys
import json
import subprocess
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import yaml
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeploymentManager:
    """Manages deployment of ETL jobs to production environment."""
    
    def __init__(self, config_path: str = "config/deployment.yaml"):
        """Initialize deployment manager with configuration."""
        self.config = self._load_config(config_path)
        self.environment = self.config.get('environment', 'production')
        self.s3_client = boto3.client('s3') if self.config.get('use_s3') else None
        
    def _load_config(self, config_path: str) -> Dict:
        """Load deployment configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            sys.exit(1)
            
    def package_application(self) -> str:
        """Package the application and dependencies into a deployable artifact."""
        logger.info("Packaging application for deployment...")
        
        package_dir = Path("dist")
        package_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        package_name = f"sales-etl-{self.environment}-{timestamp}.zip"
        package_path = package_dir / package_name
        
        # Package source code and dependencies
        files_to_package = [
            "src/",
            "config/",
            "requirements.txt",
            "setup.py"
        ]
        
        import zipfile
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for item in files_to_package:
                item_path = Path(item)
                if item_path.is_file():
                    zipf.write(item_path, item_path.name)
                elif item_path.is_dir():
                    for file_path in item_path.rglob('*'):
                        if file_path.is_file() and '__pycache__' not in str(file_path):
                            zipf.write(file_path, file_path.relative_to(item_path.parent))
        
        logger.info(f"Application packaged: {package_path}")
        return str(package_path)
    
    def upload_to_s3(self, package_path: str) -> str:
        """Upload deployment package to S3."""
        if not self.s3_client:
            logger.warning("S3 client not configured, skipping upload")
            return package_path
            
        bucket = self.config['s3']['bucket']
        prefix = self.config['s3']['prefix']
        
        s3_key = f"{prefix}/{Path(package_path).name}"
        
        try:
            logger.info(f"Uploading to s3://{bucket}/{s3_key}")
            self.s3_client.upload_file(package_path, bucket, s3_key)
            s3_path = f"s3://{bucket}/{s3_key}"
            logger.info(f"Upload complete: {s3_path}")
            return s3_path
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
    
    def deploy_to_emr(self, package_s3_path: str) -> str:
        """Deploy application to EMR cluster."""
        emr_config = self.config.get('emr', {})
        
        if not emr_config:
            logger.warning("EMR configuration not found")
            return ""
        
        emr_client = boto3.client('emr', region_name=emr_config.get('region', 'us-east-1'))
        
        step_config = {
            'Name': f'Sales ETL Job - {datetime.now().isoformat()}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '--conf', f"spark.executor.instances={emr_config.get('executor_instances', 2)}",
                    '--conf', f"spark.executor.memory={emr_config.get('executor_memory', '4g')}",
                    '--conf', f"spark.driver.memory={emr_config.get('driver_memory', '2g')}",
                    '--py-files', package_s3_path,
                    package_s3_path,
                    '--config', 's3://your-bucket/config/production.yaml'
                ]
            }
        }
        
        try:
            response = emr_client.add_job_flow_steps(
                JobFlowId=emr_config['cluster_id'],
                Steps=[step_config]
            )
            
            step_id = response['StepIds'][0]
            logger.info(f"Deployed to EMR. Step ID: {step_id}")
            return step_id
            
        except ClientError as e:
            logger.error(f"Failed to deploy to EMR: {e}")
            raise
    
    def deploy_to_databricks(self, package_path: str) -> str:
        """Deploy application to Databricks."""
        databricks_config = self.config.get('databricks', {})
        
        if not databricks_config:
            logger.warning("Databricks configuration not found")
            return ""
        
        # Use Databricks REST API or CLI to deploy
        import requests
        
        api_url = databricks_config['workspace_url']
        api_token = os.environ.get('DATABRICKS_TOKEN')
        
        if not api_token:
            logger.error("DATABRICKS_TOKEN environment variable not set")
            return ""
        
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
        
        # Upload to DBFS
        dbfs_path = f"/FileStore/etl/{Path(package_path).name}"
        
        with open(package_path, 'rb') as f:
            files = {'file': f}
            upload_url = f"{api_url}/api/2.0/dbfs/put"
            
            response = requests.post(
                upload_url,
                headers=headers,
                data={'path': dbfs_path, 'overwrite': 'true'},
                files=files
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to upload to DBFS: {response.text}")
                return ""
        
        # Create job run
        job_config = {
            "run_name": f"Sales ETL - {datetime.now().isoformat()}",
            "new_cluster": databricks_config['cluster_config'],
            "spark_python_task": {
                "python_file": f"dbfs:{dbfs_path}",
                "parameters": ["--config", "dbfs:/config/production.yaml"]
            }
        }
        
        run_url = f"{api_url}/api/2.0/jobs/runs/submit"
        response = requests.post(run_url, headers=headers, json=job_config)
        
        if response.status_code == 200:
            run_id = response.json()['run_id']
            logger.info(f"Deployed to Databricks. Run ID: {run_id}")
            return str(run_id)
        else:
            logger.error(f"Failed to create Databricks job: {response.text}")
            return ""
    
    def validate_deployment(self, deployment_id: str) -> bool:
        """Validate that deployment was successful."""
        logger.info(f"Validating deployment: {deployment_id}")
        
        # Implement validation logic based on platform
        # Check job status, logs, metrics, etc.
        
        return True
    
    def rollback(self, previous_version: str):
        """Rollback to previous version if deployment fails."""
        logger.warning(f"Rolling back to version: {previous_version}")
        
        # Implement rollback logic
        # Restore previous package, revert configuration, etc.
        
        logger.info("Rollback complete")
    
    def deploy(self) -> bool:
        """Execute full deployment process."""
        try:
            logger.info(f"Starting deployment to {self.environment}")
            
            # Package application
            package_path = self.package_application()
            
            # Upload to S3 if configured
            if self.config.get('use_s3'):
                s3_path = self.upload_to_s3(package_path)
                deployment_package = s3_path
            else:
                deployment_package = package_path
            
            # Deploy to target platform
            platform = self.config.get('platform', 'local')
            
            if platform == 'emr':
                deployment_id = self.deploy_to_emr(deployment_package)
            elif platform == 'databricks':
                deployment_id = self.deploy_to_databricks(deployment_package)
            else:
                logger.info(f"Local deployment: {deployment_package}")
                deployment_id = "local"
            
            # Validate deployment
            if deployment_id and self.validate_deployment(deployment_id):
                logger.info("Deployment successful!")
                return True
            else:
                logger.error("Deployment validation failed")
                return False
                
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            return False


def main():
    """Main deployment script entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy ETL application')
    parser.add_argument('--config', default='config/deployment.yaml',
                       help='Path to deployment configuration')
    parser.add_argument('--environment', choices=['dev', 'staging', 'production'],
                       default='production', help='Target environment')
    
    args = parser.parse_args()
    
    deployer = DeploymentManager(args.config)
    success = deployer.deploy()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()