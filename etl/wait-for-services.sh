#!/bin/bash

# usage: ./wait-for-services.sh host port service_name
wait_for() {
    echo "Waiting for $3 ($1:$2) to be ready..."
    # Loop until the TCP connection is successful
    while ! < /dev/tcp/$1/$2; do
        sleep 1
    done
    echo "$3 is up and running!"
}

# 1. Wait for the Target Database (Postgres)
# We check the host 'target_db' on port 5432
wait_for "target_db" "5432" "Target Database"

# 2. Wait for the FastAPI Service
# We check the host 'fastapi' on port 8000
wait_for "fastapi" "8000" "FastAPI Service"

echo "All services are ready. Starting Dagster..."

# Execute the command passed to this script (the dagster start command)
exec "$@"