@echo off
REM ABAP to PySpark Pipeline - Windows Startup Script

setlocal enabledelayedexpansion

echo ==========================================
echo ABAP to PySpark ETL Pipeline
echo ==========================================
echo.

REM Check if Docker is installed
where docker >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH
    echo Please install Docker Desktop from https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

REM Check if Docker Compose is installed
where docker-compose >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose is not installed or not in PATH
    echo Please install Docker Compose
    pause
    exit /b 1
)

REM Check if Docker daemon is running
docker info >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Docker daemon is not running
    echo Please start Docker Desktop
    pause
    exit /b 1
)

echo [OK] Docker is installed and running
echo.

REM Create necessary directories
echo Creating required directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "output" mkdir output
echo [OK] Directories created
echo.

REM Stop any existing containers
echo Stopping existing containers (if any)...
docker-compose down 2>nul
echo.

REM Build and start containers
echo Starting PySpark environment...
echo This may take a few minutes on first run...
docker-compose up -d

if %errorlevel% neq 0 (
    echo [ERROR] Failed to start containers
    echo Check logs with: docker-compose logs
    pause
    exit /b 1
)

REM Wait for services to be ready
echo.
echo Waiting for services to be ready...
timeout /t 10 /nobreak >nul

REM Check if containers are running
docker-compose ps | find "Up" >nul
if %errorlevel% neq 0 (
    echo [ERROR] Containers failed to start
    echo Check logs with: docker-compose logs
    pause
    exit /b 1
)

echo [OK] PySpark environment is ready
echo.

REM Display service URLs
echo ==========================================
echo Service URLs:
echo ==========================================
echo Spark Master UI: http://localhost:8080
echo Spark Worker UI: http://localhost:8081
echo Spark Application UI: http://localhost:4040
echo.

REM Execute the pipeline
echo ==========================================
echo Executing ETL Pipeline...
echo ==========================================
echo.

docker-compose exec -T pyspark python /app/main.py

set EXIT_CODE=%errorlevel%

echo.
echo ==========================================
if %EXIT_CODE% equ 0 (
    echo [SUCCESS] Pipeline execution completed successfully!
) else (
    echo [ERROR] Pipeline execution failed with exit code %EXIT_CODE%
    echo Check logs in pipeline.log or run: docker-compose logs
)
echo ==========================================
echo.

REM Ask if user wants to stop containers
set /p STOP_CONTAINERS="Do you want to stop the containers? (y/n): "
if /i "%STOP_CONTAINERS%"=="y" (
    echo Stopping containers...
    docker-compose down
    echo [OK] Containers stopped
) else (
    echo Containers are still running. To stop them later, run: docker-compose down
)

pause
exit /b %EXIT_CODE%