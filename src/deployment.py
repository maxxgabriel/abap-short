"""
Deployment automation utilities.
"""
import subprocess
import os
from typing import Dict, Any, List
import yaml
import logging


class DeploymentManager:
    """Manages deployment to production environment."""
    
    def __init__(self, config_path: str):
        """
        Initialize deployment manager.
        
        Args:
            config_path: Path to deployment configuration
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        
    def validate_environment(self) -> bool:
        """
        Validate production environment prerequisites.
        
        Returns:
            True if environment is ready
        """
        checks = []
        
        # Check Spark cluster availability
        try:
            result = subprocess.run(
                ['spark-submit', '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            checks.append(result.returncode == 0)
        except Exception as e:
            self.logger.error(f"Spark check failed: {e}")
            checks.append(False)
        
        # Check data paths exist
        for path in [self.config['paths']['source'], self.config['paths']['target']]:
            checks.append(os.path.exists(path) or path.startswith('s3://') or path.startswith('hdfs://'))
        
        return all(checks)
    
    def deploy_job(self) -> Dict[str, Any]:
        """
        Deploy ETL job to production.
        
        Returns:
            Deployment result dictionary
        """
        try:
            if not self.validate_environment():
                raise Exception("Environment validation failed")
            
            # Build deployment command
            cmd = self._build_spark_submit_command()
            
            # Execute deployment
            self.logger.info(f"Deploying with command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": ' '.join(cmd)
            }
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_spark_submit_command(self) -> List[str]:
        """Build spark-submit command with all configurations."""
        cmd = ['spark-submit']
        
        # Add Spark configurations
        spark_config = self.config['spark']['config']
        for key, value in spark_config.items():
            cmd.extend(['--conf', f'{key}={value}'])
        
        # Add deployment settings
        deploy_config = self.config['deployment']
        cmd.extend([
            '--master', deploy_config['master'],
            '--deploy-mode', deploy_config['deploy_mode'],
            '--num-executors', str(deploy_config['num_executors']),
            '--executor-memory', deploy_config['executor_memory'],
            '--executor-cores', str(deploy_config['executor_cores']),
            '--driver-memory', deploy_config['driver_memory']
        ])
        
        # Add application
        cmd.extend([
            'main.py',
            '--config', 'config.yaml',
            '--from-date', self.config['schedule']['from_date'],
            '--to-date', self.config['schedule']['to_date']
        ])
        
        return cmd