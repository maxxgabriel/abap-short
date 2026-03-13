"""
Unit tests for SparkSessionFactory
"""

import pytest
from pyspark.sql import SparkSession
from src.session_factory import SparkSessionFactory, create_session_from_config


class TestSparkSessionFactory:
    """Test cases for SparkSessionFactory."""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up SparkSession after each test."""
        yield
        SparkSessionFactory.stop_session()
    
    def test_create_new_session(self):
        """Test creating a new SparkSession."""
        session = SparkSessionFactory.get_or_create_session("test_app")
        
        assert session is not None
        assert isinstance(session, SparkSession)
        assert session.sparkContext.appName == "test_app"
    
    def test_reuse_existing_session(self):
        """Test that existing session is reused."""
        session1 = SparkSessionFactory.get_or_create_session("test_app")
        session2 = SparkSessionFactory.get_or_create_session("test_app")
        
        assert session1 is session2
    
    def test_custom_config(self):
        """Test creating session with custom configuration."""
        config = {
            "spark.sql.shuffle.partitions": "100",
            "spark.executor.memory": "2g"
        }
        
        session = SparkSessionFactory.get_or_create_session(
            "test_app",
            config=config
        )
        
        assert session.conf.get("spark.sql.shuffle.partitions") == "100"
        assert session.conf.get("spark.executor.memory") == "2g"
    
    def test_get_session_before_creation(self):
        """Test getting session before it's created returns None."""
        session = SparkSessionFactory.get_session()
        assert session is None
    
    def test_get_session_after_creation(self):
        """Test getting session after creation."""
        created = SparkSessionFactory.get_or_create_session("test_app")
        retrieved = SparkSessionFactory.get_session()
        
        assert retrieved is not None
        assert retrieved is created
    
    def test_stop_session(self):
        """Test stopping SparkSession."""
        SparkSessionFactory.get_or_create_session("test_app")
        SparkSessionFactory.stop_session()
        
        session = SparkSessionFactory.get_session()
        assert session is None
    
    def test_create_session_from_config(self):
        """Test creating session from configuration dictionary."""
        config = {
            "app_name": "config_test_app",
            "spark": {
                "spark.sql.shuffle.partitions": "50"
            }
        }
        
        session = create_session_from_config(config)
        
        assert session is not None
        assert session.sparkContext.appName == "config_test_app"
        assert session.conf.get("spark.sql.shuffle.partitions") == "50"
    
    def test_default_configurations_applied(self):
        """Test that default configurations are applied."""
        session = SparkSessionFactory.get_or_create_session("test_app")
        
        # Check some default configurations
        assert session.conf.get("spark.sql.adaptive.enabled") == "true"
        assert session.conf.get("spark.serializer") == "org.apache.spark.serializer.KryoSerializer"