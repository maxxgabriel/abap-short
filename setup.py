"""
Sales ETL System - Python Package Setup
Migrated from ABAP Package $ZETL
"""

from setuptools import setup, find_packages

setup(
    name="sales-etl-system",
    version="1.0.0",
    description="Sales Data ETL System - PySpark Implementation",
    author="ETL Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.3.0",
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0",
        "pyyaml>=6.0",
        "python-dateutil>=2.8.0",
    ],
    extras_require={
        "dev": [
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.990",
            "pre-commit>=2.20.0",
        ]
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)