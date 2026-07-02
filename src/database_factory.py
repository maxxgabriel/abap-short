"""
Database session factory for ETL operations.
Provides centralized database connection management.
"""

from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging
from pyspark.sql import SparkSession


class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass


class DatabaseSessionFactory:
    """
    Factory for creating and managing database sessions.
    Supports multiple database backends through JDBC.
    """
    
    def __init__(self, config_manager):
        """
        Initialize database factory.
        
        Args:
            config_manager: ConfigManager instance
        """
        self.config_manager = config_manager
        self.logger = logging.getLogger('etl.database')
        self._spark: Optional[SparkSession] = None
        self._connection_props: Optional[Dict[str, str]] = None
    
    def _initialize_connection_properties(self) -> Dict[str, str]:
        """Initialize JDBC connection properties."""
        db_config = self.config_manager.get_database_config()
        
        props = {
            'driver': db_config.get('driver', 'org.postgresql.Driver'),
            'user': db_config.get('username', ''),
            'password': db_config.get('password', ''),
        }
        
        # Add optional properties
        if 'schema' in db_config:
            props['currentSchema'] = db_config['schema']
        
        # Connection pool settings
        pool_config = db_config.get('connection_pool', {})
        if pool_config:
            props['numPartitions'] = str(pool_config.get('max_connections', 10))
        
        # SSL settings
        if db_config.get('ssl', False):
            props['ssl'] = 'true'
            props['sslmode'] = db_config.get('ssl_mode', 'require')
        
        return props
    
    def get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session.
        
        Returns:
            SparkSession instance
        """
        if self._spark is not None:
            return self._spark
        
        try:
            spark_config = self.config_manager.get_spark_config()
            
            builder = SparkSession.builder \
                .appName(spark_config.get('app_name', 'ETL_Process'))
            
            # Apply Spark configuration
            for key, value in spark_config.get('config', {}).items():
                builder = builder.config(key, value)
            
            # Master configuration
            if 'master' in spark_config:
                builder = builder.master(spark_config['master'])
            
            self._spark = builder.getOrCreate()
            
            self.logger.info(
                f"Spark session created: {spark_config.get('app_name')}"
            )
            
            return self._spark
            
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to create Spark session: {e}"
            )
    
    def get_connection_properties(self) -> Dict[str, str]:
        """
        Get JDBC connection properties.
        
        Returns:
            Dictionary of connection properties
        """
        if self._connection_props is None:
            self._connection_props = self._initialize_connection_properties()
        
        return self._connection_props
    
    def read_table(
        self,
        table_name: str,
        columns: Optional[list] = None,
        predicates: Optional[list] = None
    ):
        """
        Read table from database into DataFrame.
        
        Args:
            table_name: Name of table to read
            columns: Optional list of columns to select
            predicates: Optional list of predicates for partitioned reading
            
        Returns:
            DataFrame containing table data
        """
        spark = self.get_spark_session()
        connection_string = self.config_manager.get_connection_string()
        props = self.get_connection_properties()
        
        try:
            reader = spark.read.jdbc(
                url=connection_string,
                table=table_name,
                properties=props
            )
            
            # Add predicates for partitioned reading
            if predicates:
                reader = spark.read.jdbc(
                    url=connection_string,
                    table=table_name,
                    properties=props,
                    predicates=predicates
                )
            
            df = reader
            
            # Select specific columns if provided
            if columns:
                df = df.select(*columns)
            
            self.logger.info(f"Read table: {table_name}")
            return df
            
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to read table {table_name}: {e}"
            )
    
    def write_table(
        self,
        dataframe,
        table_name: str,
        mode: str = 'append',
        batch_size: Optional[int] = None
    ) -> None:
        """
        Write DataFrame to database table.
        
        Args:
            dataframe: DataFrame to write
            table_name: Target table name
            mode: Write mode (append, overwrite, error, ignore)
            batch_size: Optional batch size for inserts
        """
        connection_string = self.config_manager.get_connection_string()
        props = self.get_connection_properties()
        
        # Add batch size if provided
        if batch_size:
            props['batchsize'] = str(batch_size)
        
        try:
            dataframe.write.jdbc(
                url=connection_string,
                table=table_name,
                mode=mode,
                properties=props
            )
            
            self.logger.info(
                f"Written {dataframe.count()} records to {table_name}"
            )
            
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to write to table {table_name}: {e}"
            )
    
    def execute_query(self, query: str):
        """
        Execute SQL query and return DataFrame.
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        spark = self.get_spark_session()
        connection_string = self.config_manager.get_connection_string()
        props = self.get_connection_properties()
        
        try:
            df = spark.read.jdbc(
                url=connection_string,
                table=f"({query}) as query",
                properties=props
            )
            
            self.logger.info("Query executed successfully")
            return df
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to execute query: {e}")
    
    @contextmanager
    def get_session(self):
        """
        Context manager for database session.
        
        Yields:
            Tuple of (SparkSession, connection_properties)
        """
        spark = self.get_spark_session()
        props = self.get_connection_properties()
        
        try:
            yield spark, props
        finally:
            # Cleanup if needed
            pass
    
    def close(self) -> None:
        """Close Spark session and cleanup resources."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            self.logger.info("Spark session closed")


class TransactionManager:
    """Manages database transactions for ETL operations."""
    
    def __init__(self, session_factory: DatabaseSessionFactory):
        """
        Initialize transaction manager.
        
        Args:
            session_factory: DatabaseSessionFactory instance
        """
        self.session_factory = session_factory
        self.logger = logging.getLogger('etl.transaction')
    
    @contextmanager
    def transaction(self):
        """
        Context manager for database transaction.
        
        Note: Spark doesn't support traditional transactions.
        This provides a pattern for atomic operations.
        """
        spark = self.session_factory.get_spark_session()
        
        try:
            # Start transaction context
            self.logger.debug("Transaction started")
            yield spark
            
            # Commit (implicit in Spark)
            self.logger.debug("Transaction committed")
            
        except Exception as e:
            # Rollback (limited in Spark)
            self.logger.error(f"Transaction failed: {e}")
            raise
    
    def update_source_status(
        self,
        table_name: str,
        id_column: str,
        ids: list,
        status: str
    ) -> None:
        """
        Update status of source records.
        
        Args:
            table_name: Source table name
            id_column: ID column name
            ids: List of IDs to update
            status: New status value
        """
        # In production, use proper SQL UPDATE
        # This is a simplified implementation
        self.logger.info(
            f"Updating {len(ids)} records in {table_name} to status {status}"
        )