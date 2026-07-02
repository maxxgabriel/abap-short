"""
Spark Session Factory Module
Provides centralized SparkSession creation and management for ETL modules.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging


class SparkSessionFactory:
    """Factory class for creating and managing Spark sessions."""
    
    _instance: Optional[SparkSession] = None
    _logger = logging.getLogger(__name__)
    
    @classmethod
    def get_or_create_session(
        cls,
        app_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        """
        Get existing SparkSession or create new one.
        
        Args:
            app_name: Name of the Spark application
            config: Optional dictionary of Spark configuration parameters
            
        Returns:
            SparkSession instance
        """
        if cls._instance is None:
            cls._logger.info(f"Creating new SparkSession: {app_name}")
            builder = SparkSession.builder.appName(app_name)
            
            # Apply default configurations
            default_config = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.shuffle.partitions": "200",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.sources.partitionOverwriteMode": "dynamic"
            }
            
            # Merge with provided config
            if config:
                default_config.update(config)
            
            # Apply configurations
            for key, value in default_config.items():
                builder = builder.config(key, value)
            
            cls._instance = builder.getOrCreate()
            cls._logger.info("SparkSession created successfully")
        else:
            cls._logger.debug("Reusing existing SparkSession")
        
        return cls._instance
    
    @classmethod
    def stop_session(cls) -> None:
        """Stop the current SparkSession if it exists."""
        if cls._instance is not None:
            cls._logger.info("Stopping SparkSession")
            cls._instance.stop()
            cls._instance = None
            cls._logger.info("SparkSession stopped")
    
    @classmethod
    def get_session(cls) -> Optional[SparkSession]:
        """
        Get the current SparkSession without creating a new one.
        
        Returns:
            Current SparkSession or None if not initialized
        """
        return cls._instance


def create_session_from_config(config: Dict[str, Any]) -> SparkSession:
    """
    Create SparkSession from configuration dictionary.
    
    Args:
        config: Configuration dictionary containing spark settings
        
    Returns:
        SparkSession instance
    """
    app_name = config.get("app_name", "ETL_Application")
    spark_config = config.get("spark", {})
    
    return SparkSessionFactory.get_or_create_session(
        app_name=app_name,
        config=spark_config
    )