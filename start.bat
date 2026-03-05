@echo off
setlocal enabledelayedexpansion

echo ==========================================
echo ABAP Short - PySpark Pipeline Launcher
echo ==========================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not running. Please start Docker and try again.
    pause
    exit /b 1
)

REM Check if docker-compose is available
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo Error: docker-compose is not installed. Please install docker-compose and try again.
    pause
    exit /b 1
)

echo [32m✓ Docker is running[0m
echo.

REM Start Docker Compose services
echo Starting PySpark environment...
docker-compose up -d

if errorlevel 1 (
    echo Error: Failed to start Docker environment
    docker-compose logs
    pause
    exit /b 1
)

REM Wait for container to be ready
echo Waiting for container to be ready...
timeout /t 10 /nobreak >nul

REM Check if container is running
docker ps | findstr "abap_short_pyspark" >nul
if errorlevel 1 (
    echo Error: Container failed to start
    docker-compose logs
    pause
    exit /b 1
)

echo [32m✓ PySpark environment is ready[0m
echo.

REM Install dependencies
echo Installing dependencies...
docker exec abap_short_pyspark bash -c "pip install --upgrade pip && pip install -r requirements.txt"

if errorlevel 1 (
    echo Error: Failed to install dependencies
    docker-compose logs
    pause
    exit /b 1
)

echo [32m✓ Dependencies installed[0m
echo.

REM Run the pipeline
echo ==========================================
echo Running Pipeline
echo ==========================================
echo.

docker exec -it abap_short_pyspark python main.py

set PIPELINE_EXIT_CODE=%errorlevel%

echo.
echo ==========================================
if %PIPELINE_EXIT_CODE% equ 0 (
    echo [32m✓ Pipeline completed successfully[0m
) else (
    echo [31m✗ Pipeline failed with exit code %PIPELINE_EXIT_CODE%[0m
)
echo ==========================================
echo.

REM Show logs location
echo Logs are available in: .\logs
echo Output is available in: .\output
echo.

REM Ask if user wants to stop the environment
set /p STOP_ENV="Stop the Docker environment? (y/n): "
if /i "!STOP_ENV!"=="y" (
    echo Stopping Docker environment...
    docker-compose down
    echo [32m✓ Environment stopped[0m
) else (
    echo Environment is still running. To stop it later, run: docker-compose down
    echo To view logs: docker-compose logs -f
    echo To access container shell: docker exec -it abap_short_pyspark bash
)

echo.
pause
exit /b %PIPELINE_EXIT_CODE%