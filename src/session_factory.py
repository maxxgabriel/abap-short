"""
Spark Session Factory
Centralized Spark session management with database connections
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging

from src.config_manager import config

logger = logging.getLogger(__name__)


class SessionFactory:
    """Factory for creating and managing Spark sessions"""
    
    _session: Optional[SparkSession] = None
    
    @classmethod
    def get_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create Spark session
        
        Args:
            app_name: Application name (uses config if not provided)
            
        Returns:
            Configured SparkSession
        """
        if cls._session is None:
            cls._session = cls._create_session(app_name)
        
        return cls._session
    
    @classmethod
    def _create_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Create new Spark session with configuration
        
        Args:
            app_name: Application name
            
        Returns:
            Configured SparkSession
        """
        if app_name is None:
            app_name = config.get('spark.app_name', 'ETL Application')
        
        # Get Spark configuration
        master = config.get('spark.master', 'local[*]')
        spark_config = config.get_spark_config()
        
        logger.info(f"Creating Spark session: {app_name}")
        logger.info(f"Master: {master}")
        
        # Build Spark session
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)
        
        # Add configuration
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # Add JDBC drivers (PostgreSQL example)
        builder = builder.config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.0"
        )
        
        session = builder.getOrCreate()
        
        # Set log level
        log_level = config.get('spark.log_level', 'WARN')
        session.sparkContext.setLogLevel(log_level)
        
        logger.info("Spark session created successfully")
        
        return session
    
    @classmethod
    def stop_session(cls) -> None:
        """Stop current Spark session"""
        if cls._session is not None:
            logger.info("Stopping Spark session")
            cls._session.stop()
            cls._session = None
    
    @classmethod
    def get_jdbc_reader(
        cls,
        db_type: str = 'source'
    ) -> 'pyspark.sql.DataFrameReader':
        """
        Get configured JDBC reader
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Configured DataFrameReader
        """
        session = cls.get_session()
        db_config = config.get_database_config(db_type)
        
        reader = session.read \
            .format("jdbc") \
            .option("url", db_config['url']) \
            .option("driver", db_config['driver']) \
            .option("user", db_config['user']) \
            .option("password", db_config['password'])
        
        # Add additional properties
        properties = db_config.get('properties', {})
        for key, value in properties.items():
            reader = reader.option(key, value)
        
        return reader
    
    @classmethod
    def get_jdbc_writer(
        cls,
        dataframe,
        db_type: str = 'target',
        mode: str = 'append'
    ) -> 'pyspark.sql.DataFrameWriter':
        """
        Get configured JDBC writer
        
        Args:
            dataframe: DataFrame to write
            db_type: 'source' or 'target'
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Configured DataFrameWriter
        """
        db_config = config.get_database_config(db_type)
        
        writer = dataframe.write \
            .format("jdbc") \
            .mode(mode) \
            .option("url", db_config['url']) \
            .option("driver", db_config['driver']) \
            .option("user", db_config['user']) \
            .option("password", db_config['password'])
        
        # Add additional properties
        properties = db_config.get('properties', {})
        for key, value in properties.items():
            writer = writer.option(key, value)
        
        return writer
    
    @classmethod
    def read_table(
        cls,
        table_name: str,
        db_type: str = 'source',
        query: Optional[str] = None
    ):
        """
        Read table from database
        
        Args:
            table_name: Table name
            db_type: 'source' or 'target'
            query: Optional SQL query (overrides table_name)
            
        Returns:
            DataFrame
        """
        reader = cls.get_jdbc_reader(db_type)
        
        if query:
            # Use custom query
            df = reader.option("query", query).load()
        else:
            # Read full table
            df = reader.option("dbtable", table_name).load()
        
        logger.info(f"Read table: {table_name} from {db_type} database")
        
        return df
    
    @classmethod
    def write_table(
        cls,
        dataframe,
        table_name: str,
        db_type: str = 'target',
        mode: str = 'append'
    ) -> None:
        """
        Write DataFrame to database table
        
        Args:
            dataframe: DataFrame to write
            table_name: Target table name
            db_type: 'source' or 'target'
            mode: Write mode
        """
        writer = cls.get_jdbc_writer(dataframe, db_type, mode)
        writer.option("dbtable", table_name).save()
        
        logger.info(f"Wrote to table: {table_name} in {db_type} database (mode: {mode})")
    
    @classmethod
    def get_session_info(cls) -> Dict[str, Any]:
        """
        Get information about current Spark session
        
        Returns:
            Dictionary with session information
        """
        if cls._session is None:
            return {"status": "No active session"}
        
        conf = cls._session.sparkContext.getConf()
        
        return {
            "status": "Active",
            "app_name": cls._session.sparkContext.appName,
            "app_id": cls._session.sparkContext.applicationId,
            "master": conf.get("spark.master"),
            "spark_version": cls._session.version,
            "python_version": cls._session.sparkContext.pythonVer,
            "configs": dict(conf.getAll())
        }


# Convenience function
def get_spark() -> SparkSession:
    """Get Spark session (convenience function)"""
    return SessionFactory.get_session()