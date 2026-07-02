```
@echo off
echo Starting Sales ETL System...

REM Start Docker containers
echo Starting Docker containers...
docker-compose up -d

REM Wait for services to be ready
echo Waiting for services to start...
timeout /t 10 /nobreak > nul

REM Check if containers are running
docker ps | findstr spark-master > nul
if errorlevel 1 (
    echo Error: Spark master container is not running
    exit /b 1
)

echo Services started successfully
echo.
echo Spark Master UI: http://localhost:8080
echo PostgreSQL: localhost:5432
echo.

REM Run ETL process
echo Running ETL process...
docker exec spark-master spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    /opt/spark-apps/src/main.py ^
    --config /opt/spark-apps/config/business_rules.yaml ^
    %*

echo.
echo ETL process completed
```