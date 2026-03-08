@echo off
REM Start script for ABAP to PySpark migrated project (Windows)
REM This script starts the Docker environment and runs the pipeline

echo ==================================================
echo ABAP to PySpark Migration - Startup Script
echo ==================================================
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not installed or not in PATH
    echo Please install Docker Desktop from https://www.docker.com/products/docker-desktop
    exit /b 1
)

REM Check if Docker Compose is available
docker-compose --version >nul 2>&1
if errorlevel 1 (
    docker compose version >nul 2>&1
    if errorlevel 1 (
        echo ERROR: Docker Compose is not installed
        echo Please install Docker Compose
        exit /b 1
    )
    set DOCKER_COMPOSE=docker compose
) else (
    set DOCKER_COMPOSE=docker-compose
)

echo Step 1: Creating required directories...
if not exist data mkdir data
if not exist logs mkdir logs
if not exist checkpoints mkdir checkpoints
if not exist output mkdir output

echo Step 2: Starting Docker containers...
%DOCKER_COMPOSE% up -d

echo Step 3: Waiting for Spark to be ready...
timeout /t 10 /nobreak >nul

echo Step 4: Installing Python dependencies...
docker exec abap_test_pyspark pip install --no-cache-dir -r /app/requirements.txt

echo Step 5: Running the PySpark pipeline...
echo.
docker exec abap_test_pyspark python /app/main.py

echo.
echo ==================================================
echo Pipeline execution completed!
echo ==================================================
echo.
echo Useful commands:
echo   View logs:        docker logs abap_test_pyspark
echo   Stop containers:  %DOCKER_COMPOSE% down
echo   Restart:          %DOCKER_COMPOSE% restart
echo   Shell access:     docker exec -it abap_test_pyspark bash
echo   Spark UI:         http://localhost:8080
echo.

pause