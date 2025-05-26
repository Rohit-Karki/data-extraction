#!/bin/bash

# Exit on error
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
while ! nc -z hive-metastore-postgresql 5432; do
  sleep 1
done
echo "PostgreSQL started"

# Initialize the Hive Metastore schema
echo "Initializing Hive Metastore schema..."
schematool -initSchema -dbType postgres || echo "Schema already exists, continuing..."

# Start the Hive Metastore service in foreground mode
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore