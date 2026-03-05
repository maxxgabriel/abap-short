"""
Database and Spark Session Factory
Manages creation and lifecycle of database connections and Spark sessions
"""
from typing import Optional, Dict, Any
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class SparkSessionFactory:
    """Factory for creating and managing Spark sessions"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def create_session(
        cls,
        app_name: str = "ETL_Application",
        master: str = "local[*]",
        config: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        """
        Create or get existing Spark session
        
        Args:
            app_name: Application name
            master: Spark master URL
            config: Additional Spark configuration
            
        Returns:
            SparkSession instance
        """
        if cls._instance is not None:
            return cls._instance
        
        # Build Spark configuration
        spark_conf = SparkConf()
        spark_conf.setAppName(app_name)
        spark_conf.setMaster(master)
        
        # Default configurations
        default_config = {
            'spark.sql.shuffle.partitions': '200',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.sources.partitionOverwriteMode': 'dynamic',
            'spark.sql.session.timeZone': 'UTC'
        }
        
        # Merge with provided config
        if config:
            default_config.update(config)
        
        # Set all configurations
        for key, value in default_config.items():
            spark_conf.set(key, value)
        
        # Create Spark session
        builder = SparkSession.builder.config(conf=spark_conf)
        
        # Enable Hive support if needed
        if config and config.get('enable_hive_support', False):
            builder = builder.enableHiveSupport()
        
        cls._instance = builder.getOrCreate()
        
        # Set log level
        log_level = config.get('log_level', 'WARN') if config else 'WARN'
        cls._instance.sparkContext.setLogLevel(log_level)
        
        return cls._instance
    
    @classmethod
    def get_session(cls) -> Optional[SparkSession]:
        """Get existing Spark session"""
        return cls._instance
    
    @classmethod
    def stop_session(cls) -> None:
        """Stop Spark session"""
        if cls._instance:
            cls._instance.stop()
            cls._instance = None


class DatabaseSessionFactory:
    """Factory for managing database connections through Spark"""
    
    @staticmethod
    def create_jdbc_reader(
        spark: SparkSession,
        jdbc_url: str,
        table: str,
        user: str,
        password: str,
        driver: str = "org.postgresql.Driver",
        properties: Optional[Dict[str, str]] = None
    ):
        """
        Create JDBC reader for database table
        
        Args:
            spark: SparkSession instance
            jdbc_url: JDBC connection URL
            table: Table name
            user: Database user
            password: Database password
            driver: JDBC driver class
            properties: Additional JDBC properties
            
        Returns:
            DataFrameReader configured for JDBC
        """
        jdbc_properties = {
            'user': user,
            'password': password,
            'driver': driver
        }
        
        if properties:
            jdbc_properties.update(properties)
        
        return spark.read \
            .format('jdbc') \
            .option('url', jdbc_url) \
            .option('dbtable', table) \
            .options(**jdbc_properties)
    
    @staticmethod
    def create_jdbc_writer(
        spark: SparkSession,
        jdbc_url: str,
        table: str,
        user: str,
        password: str,
        driver: str = "org.postgresql.Driver",
        mode: str = "append",
        properties: Optional[Dict[str, str]] = None
    ):
        """
        Create JDBC writer for database table
        
        Args:
            spark: SparkSession instance
            jdbc_url: JDBC connection URL
            table: Table name
            user: Database user
            password: Database password
            driver: JDBC driver class
            mode: Write mode (append, overwrite, ignore, error)
            properties: Additional JDBC properties
            
        Returns:
            Configured write function
        """
        jdbc_properties = {
            'user': user,
            'password': password,
            'driver': driver
        }
        
        if properties:
            jdbc_properties.update(properties)
        
        def write_dataframe(df):
            df.write \
                .format('jdbc') \
                .option('url', jdbc_url) \
                .option('dbtable', table) \
                .options(**jdbc_properties) \
                .mode(mode) \
                .save()
        
        return write_dataframe
    
    @staticmethod
    @contextmanager
    def jdbc_connection(
        spark: SparkSession,
        jdbc_url: str,
        user: str,
        password: str,
        driver: str = "org.postgresql.Driver"
    ):
        """
        Context manager for JDBC connections
        
        Args:
            spark: SparkSession instance
            jdbc_url: JDBC connection URL
            user: Database user
            password: Database password
            driver: JDBC driver class
            
        Yields:
            Connection properties dictionary
        """
        properties = {
            'user': user,
            'password': password,
            'driver': driver,
            'url': jdbc_url
        }
        
        try:
            yield properties
        finally:
            pass  # Spark manages connection lifecycle


class SessionManager:
    """Unified session manager for Spark and database"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize session manager
        
        Args:
            config: Configuration dictionary containing spark and database settings
        """
        self.config = config
        self.spark_session: Optional[SparkSession] = None
    
    def initialize(self) -> SparkSession:
        """
        Initialize Spark session with configuration
        
        Returns:
            SparkSession instance
        """
        spark_config = self.config.get('spark', {})
        
        self.spark_session = SparkSessionFactory.create_session(
            app_name=spark_config.get('app_name', 'SalesETL'),
            master=spark_config.get('master', 'local[*]'),
            config={
                'spark.executor.memory': spark_config.get('executor_memory', '2g'),
                'spark.driver.memory': spark_config.get('driver_memory', '1g'),
                'spark.sql.shuffle.partitions': str(spark_config.get('shuffle_partitions', 200)),
                'spark.dynamicAllocation.enabled': str(spark_config.get('dynamic_allocation_enabled', True)).lower(),
                'log_level': spark_config.get('log_level', 'WARN')
            }
        )
        
        return self.spark_session
    
    def get_spark_session(self) -> SparkSession:
        """Get Spark session"""
        if self.spark_session is None:
            self.initialize()
        return self.spark_session
    
    def get_jdbc_reader(self, table: str):
        """Get JDBC reader for specified table"""
        db_config = self.config.get('database', {})
        
        return DatabaseSessionFactory.create_jdbc_reader(
            spark=self.get_spark_session(),
            jdbc_url=db_config.get('jdbc_url'),
            table=table,
            user=db_config.get('user'),
            password=db_config.get('password'),
            driver=db_config.get('driver', 'org.postgresql.Driver')
        )
    
    def get_jdbc_writer(self, table: str, mode: str = 'append'):
        """Get JDBC writer for specified table"""
        db_config = self.config.get('database', {})
        
        return DatabaseSessionFactory.create_jdbc_writer(
            spark=self.get_spark_session(),
            jdbc_url=db_config.get('jdbc_url'),
            table=table,
            user=db_config.get('user'),
            password=db_config.get('password'),
            driver=db_config.get('driver', 'org.postgresql.Driver'),
            mode=mode
        )
    
    def shutdown(self) -> None:
        """Shutdown all sessions"""
        if self.spark_session:
            SparkSessionFactory.stop_session()
            self.spark_session = None