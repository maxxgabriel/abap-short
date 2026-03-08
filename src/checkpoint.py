"""Checkpoint management for ETL pipeline."""
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession


class CheckpointManager:
    """Manages checkpoints for fault tolerance."""
    
    def __init__(self, spark: SparkSession, config: dict, etl_run_id: str):
        """Initialize checkpoint manager.
        
        Args:
            spark: SparkSession instance
            config: Checkpoint configuration
            etl_run_id: Unique identifier for ETL run
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = etl_run_id
        self.checkpoint_enabled = config.get('checkpoints', {}).get('enabled', True)
        self.base_path = Path(config.get('checkpoints', {}).get('base_path', 'data/checkpoints'))
        self.stages = config.get('checkpoints', {}).get('stages', [])
        
        if self.checkpoint_enabled:
            self._ensure_checkpoint_dir()
    
    def _ensure_checkpoint_dir(self) -> None:
        """Ensure checkpoint directory exists."""
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_checkpoint_path(self, stage: str) -> Path:
        """Get checkpoint path for a stage.
        
        Args:
            stage: Pipeline stage name
            
        Returns:
            Path to checkpoint file
        """
        return self.base_path / self.etl_run_id / f"{stage}.json"
    
    def _get_data_checkpoint_path(self, stage: str) -> str:
        """Get data checkpoint path for a stage.
        
        Args:
            stage: Pipeline stage name
            
        Returns:
            Path to checkpoint data directory
        """
        return str(self.base_path / self.etl_run_id / stage / "data")
    
    def save_checkpoint(
        self,
        stage: str,
        metadata: Dict[str, Any],
        data_path: Optional[str] = None
    ) -> None:
        """Save checkpoint for a stage.
        
        Args:
            stage: Pipeline stage name
            metadata: Checkpoint metadata
            data_path: Optional path to saved data
        """
        if not self.checkpoint_enabled or stage not in self.stages:
            return
        
        checkpoint_path = self._get_checkpoint_path(stage)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint_data = {
            'stage': stage,
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata,
            'data_path': data_path,
            'status': 'completed'
        }
        
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def load_checkpoint(self, stage: str) -> Optional[Dict[str, Any]]:
        """Load checkpoint for a stage.
        
        Args:
            stage: Pipeline stage name
            
        Returns:
            Checkpoint data if exists, None otherwise
        """
        if not self.checkpoint_enabled or stage not in self.stages:
            return None
        
        checkpoint_path = self._get_checkpoint_path(stage)
        
        if not checkpoint_path.exists():
            return None
        
        with open(checkpoint_path, 'r') as f:
            return json.load(f)
    
    def checkpoint_exists(self, stage: str) -> bool:
        """Check if checkpoint exists for a stage.
        
        Args:
            stage: Pipeline stage name
            
        Returns:
            True if checkpoint exists, False otherwise
        """
        if not self.checkpoint_enabled or stage not in self.stages:
            return False
        
        return self._get_checkpoint_path(stage).exists()
    
    def clear_checkpoint(self, stage: str) -> None:
        """Clear checkpoint for a stage.
        
        Args:
            stage: Pipeline stage name
        """
        if not self.checkpoint_enabled:
            return
        
        checkpoint_path = self._get_checkpoint_path(stage)
        if checkpoint_path.exists():
            checkpoint_path.unlink()
    
    def clear_all_checkpoints(self) -> None:
        """Clear all checkpoints for current run."""
        if not self.checkpoint_enabled:
            return
        
        run_checkpoint_dir = self.base_path / self.etl_run_id
        if run_checkpoint_dir.exists():
            import shutil
            shutil.rmtree(run_checkpoint_dir)