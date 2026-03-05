@echo off
REM start.bat - Launch script for ABAP to PySpark Migration (Windows)

echo ================================================
echo ABAP to PySpark Migration - Startup Script
echo ================================================
echo.

REM Create necessary directories
echo Creating necessary directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "output" mkdir output
if not exist "config" mkdir config

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not installed. Please install Docker Desktop first.
    pause
    exit /b 1
)

REM Check if Docker Compose is installed
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo Error: Docker Compose is not installed. Please install Docker Compose first.
    pause
    exit /b 1
)

REM Check if Docker daemon is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker daemon is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

REM Stop any existing containers
echo Stopping any existing containers...
docker-compose down 2>nul

REM Build and start the Docker environment
echo Starting Docker environment...
docker-compose up -d

if errorlevel 1 (
    echo Error: Failed to start Docker environment
    pause
    exit /b 1
)

REM Wait for the container to be ready
echo Waiting for PySpark environment to be ready...
timeout /t 10 /nobreak >nul

REM Check if container is running
docker ps | findstr pyspark_abap_migration >nul
if errorlevel 1 (
    echo Error: PySpark container failed to start
    docker-compose logs
    pause
    exit /b 1
)

echo PySpark environment is ready!
echo.

REM Execute the main pipeline
echo Running the migration pipeline...
docker exec -it pyspark_abap_migration python main.py

if errorlevel 1 (
    echo ================================================
    echo Pipeline execution failed!
    echo ================================================
    echo Check logs in .\logs\ directory
    pause
    exit /b 1
) else (
    echo ================================================
    echo Pipeline completed successfully!
    echo ================================================
)

echo.
echo Logs available in: .\logs\
echo Output available in: .\output\
echo.

echo Options:
echo   - To view logs: docker-compose logs -f
echo   - To stop environment: docker-compose down
echo   - To access PySpark shell: docker exec -it pyspark_abap_migration pyspark
echo   - To access container: docker exec -it pyspark_abap_migration bash
echo.

echo Container is still running for inspection. Use 'docker-compose down' to stop.
echo.
pause