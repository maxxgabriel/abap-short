```
#!/bin/bash

echo "Starting Sales ETL System..."

# Start Docker containers
echo "Starting Docker containers..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Check if containers are running
if ! docker ps | grep -q spark-master; then
    echo "Error: Spark master container is not running"
    exit 1
fi

echo "Services started successfully"
echo ""
echo "Spark Master UI: http://localhost:8080"
echo "PostgreSQL: localhost:5432"
echo ""

# Run ETL process
echo "Running ETL process..."
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-apps/src/main.py \
    --config /opt/spark-apps/config/business_rules.yaml \
    "$@"

echo ""
echo "ETL process completed"
```