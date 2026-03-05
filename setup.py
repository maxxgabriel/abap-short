"""
Setup configuration for Sales ETL System
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements from file
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="sales-etl-system",
    version="1.0.0",
    author="Senior PySpark Team",
    author_email="dev@example.com",
    description="Sales ETL System - Python/PySpark Implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/sales-etl-system",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: ETL",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sales-etl=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["config.yaml"],
    },
    zip_safe=False,
)