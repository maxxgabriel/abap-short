"""
Sales ETL System - PySpark Migration
Package setup and installation configuration
"""

from setuptools import setup, find_packages

setup(
    name="sales-etl-system",
    version="1.0.0",
    description="Sales Data ETL System - Migrated from ABAP to PySpark",
    author="ETL Team",
    author_email="etl-team@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0",
        "pyyaml>=6.0",
        "python-dateutil>=2.8.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-spark>=0.6.0",
            "black>=22.0.0",
            "pylint>=2.15.0",
            "mypy>=0.990",
        ],
        "prod": [
            "pyspark[sql]>=3.3.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "sales-etl=main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)