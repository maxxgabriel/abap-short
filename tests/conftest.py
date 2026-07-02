"""
Pytest configuration and fixtures.
"""

import pytest
import shutil
import os


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_data():
    """Clean up test data before and after test session."""
    test_paths = [
        '/tmp/test_zetl_log',
        '/tmp/test_delta_log'
    ]
    
    # Clean up before tests
    for path in test_paths:
        if os.path.exists(path):
            shutil.rmtree(path)
    
    yield
    
    # Clean up after tests
    for path in test_paths:
        if os.path.exists(path):
            shutil.rmtree(path)