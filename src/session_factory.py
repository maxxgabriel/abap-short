"""
Session Factory Module
Database session factory for connection pooling and transaction management.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import logging
from contextlib import contextmanager


class SparkSessionFactory:
    """
    Factory for creating and managing Spark sessions with connection pooling.
    Implements singleton pattern for session reuse.
    """
    
    _instance: Optional['SparkSessionFactory'] = None
    _spark_session: Optional[SparkSession] = None
    
    def __new__(cls):
        """Singleton pattern to ensure single factory instance."""
        if cls._instance is None:
            cls._instance = super(SparkSessionFactory, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize session factory."""
        self.logger = logging.getLogger(__name__)
    
    def create_session(self, config: Optional[Dict[str, Any]] = None) -> SparkSession:
        """
        Create or retrieve Spark session.
        
        Args:
            config: Optional configuration dictionary
            
        Returns:
            SparkSession instance
        """
        if self._spark_session is not None:
            self.logger.info("Reusing existing Spark session")
            return self._spark_session
        
        if config is None:
            from src.config_manager import config as cfg
            config = cfg.get_spark_config()
        
        self.logger.info("Creating new Spark session")
        
        builder = SparkSession.builder
        
        # Set application name
        app_name = config.get('app_name', 'ETL_Application')
        builder = builder.appName(app_name)
        
        # Set master
        master = config.get('master', 'local[*]')
        builder = builder.master(master)
        
        # Set Spark configurations
        spark_configs = config.get('configs', {})
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        # Enable Hive support if configured
        if config.get('enable_hive_support', False):
            builder = builder.enableHiveSupport()
        
        # Create session
        self._spark_session = builder.getOrCreate()
        
        # Set log level
        log_level = config.get('log_level', 'WARN')
        self._spark_session.sparkContext.setLogLevel(log_level)
        
        self.logger.info(f"Spark session created: {app_name}")
        
        return self._spark_session
    
    def get_session(self) -> Optional[SparkSession]:
        """
        Get existing Spark session.
        
        Returns:
            SparkSession instance or None if not created
        """
        return self._spark_session
    
    def stop_session(self) -> None:
        """Stop the current Spark session."""
        if self._spark_session is not None:
            self.logger.info("Stopping Spark session")
            self._spark_session.stop()
            self._spark_session = None
    
    @contextmanager
    def get_transaction_context(self):
        """
        Context manager for transactional operations.
        
        Yields:
            SparkSession instance
        """
        session = self.get_session()
        if session is None:
            session = self.create_session()
        
        try:
            yield session
        except Exception as e:
            self.logger.error(f"Transaction error: {str(e)}")
            raise
        finally:
            # Spark doesn't have explicit transactions like traditional databases
            # but we can ensure checkpoints or actions are completed
            pass


class DatabaseConnectionManager:
    """
    Manager for database connections with connection pooling.
    Handles JDBC and other database connections.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize connection manager.
        
        Args:
            config: Optional database configuration
        """
        if config is None:
            from src.config_manager import config as cfg
            config = cfg.get_database_config()
        
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_jdbc_properties(self) -> Dict[str, str]:
        """
        Get JDBC connection properties.
        
        Returns:
            Dictionary of JDBC properties
        """
        jdbc_config = self.config.get('jdbc', {})
        
        properties = {
            'user': jdbc_config.get('user', ''),
            'password': jdbc_config.get('password', ''),
            'driver': jdbc_config.get('driver', 'org.postgresql.Driver')
        }
        
        # Add connection pool settings
        pool_config = jdbc_config.get('connection_pool', {})
        if pool_config:
            properties.update({
                'numPartitions': str(pool_config.get('max_connections', 10)),
                'fetchsize': str(pool_config.get('fetch_size', 1000))
            })
        
        return properties
    
    def get_jdbc_url(self) -> str:
        """
        Get JDBC connection URL.
        
        Returns:
            JDBC URL string
        """
        jdbc_config = self.config.get('jdbc', {})
        
        host = jdbc_config.get('host', 'localhost')
        port = jdbc_config.get('port', 5432)
        database = jdbc_config.get('database', 'etl_db')
        
        return f"jdbc:postgresql://{host}:{port}/{database}"
    
    def read_table(self, spark: SparkSession, table_name: str, **kwargs):
        """
        Read data from database table.
        
        Args:
            spark: SparkSession instance
            table_name: Name of the table to read
            **kwargs: Additional read options
            
        Returns:
            DataFrame with table data
        """
        self.logger.info(f"Reading table: {table_name}")
        
        return spark.read \
            .format("jdbc") \
            .option("url", self.get_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("driver", self.get_jdbc_properties()['driver']) \
            .options(**self.get_jdbc_properties()) \
            .options(**kwargs) \
            .load()
    
    def write_table(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Write DataFrame to database table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode (append, overwrite, etc.)
            **kwargs: Additional write options
        """
        self.logger.info(f"Writing to table: {table_name} (mode: {mode})")
        
        df.write \
            .format("jdbc") \
            .option("url", self.get_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("driver", self.get_jdbc_properties()['driver']) \
            .options(**self.get_jdbc_properties()) \
            .mode(mode) \
            .options(**kwargs) \
            .save()


# Global session factory instance
session_factory = SparkSessionFactory()