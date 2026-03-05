"""
Pytest configuration and shared fixtures
"""

import pytest
import os


def pytest_configure(config):
    """Configure pytest environment"""
    # Set environment variables for testing
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


@pytest.fixture(scope="session")
def test_config():
    """Test configuration dictionary"""
    return {
        "delta_table_path": "/tmp/test_etl_logs",
        "user": "test_user",
        "console_logging": False,
        "log_level": "INFO"
    }