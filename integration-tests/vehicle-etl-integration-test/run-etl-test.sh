#!/bin/bash

# Script to run the ETL environment and test
set -e

function pre_test() {
  echo "==== PRE-TEST PHASE ===="
  echo "Starting Docker Compose environment..."
  docker compose -f docker-compose-spark.yml down -v

  # Start only the persistent services
  docker compose -f docker-compose-spark.yml up -d hive-metastore

  echo "Waiting for Hive Metastore to start..."
  sleep 30

  echo "Pre-test phase completed."
}

function run_etl() {
  echo "==== ETL EXECUTION PHASE ===="
  echo "Running ETL job..."

  # Use docker-compose run to ensure we use the right image
  docker compose -f docker-compose-spark.yml run --rm \
    -v $(pwd)/sql:/app/sql \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/vehicle-etl.jar:/app/vehicle-etl.jar \
    spark-runner bash -c '
    echo "Waiting for Hive Metastore..." &&
    sleep 5 &&

    # Initialize tables using Spark and external SQL file
    # Modified SQL file would reference /app/data/vehicles.csv directly
    /opt/spark/bin/spark-sql --master local[1] \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
      --conf spark.sql.warehouse.dir=/user/hive/warehouse \
      -f /app/sql/init-tables.sql &&

    echo "Tables initialized. Running ETL job..." &&
    chmod 755 /app/vehicle-etl.jar &&

    # Create the output directory that the ETL job expects
    mkdir -p /tmp/vehicle-data/v001/date=2021-04-01/hour=0 &&
    mkdir -p /tmp/vehicle-data/v001/date=2021-04-02/hour=0 &&
    mkdir -p /tmp/vehicle-data/v001/date=2021-04-03/hour=0 &&

    # Run the ETL job with verbose output
    /opt/spark/bin/spark-submit --master local[1] \
      --class com.insite.etl.common.ETLRunner \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
      --conf spark.sql.warehouse.dir=/user/hive/warehouse \
      --conf spark.driver.extraJavaOptions="-Dlog4j.logger.org=INFO" \
      /app/vehicle-etl.jar com.insite.etl.vehicle.VehicleETL partition-columns=date,hour base-path=/tmp/vehicle-data target-table=vehicle_datalake.processed_vehicles version=v001 &&

    # Add some diagnostic commands to verify what happened
    echo "ETL job completed. Checking output directory..." &&
    ls -la /tmp/vehicle-data/v001/ &&
    echo "Checking partition directories..." &&
    find /tmp/vehicle-data -type d | sort
  '

  if [ $? -ne 0 ]; then
    echo "ETL execution failed!"
    exit 1
  fi

  echo "ETL execution completed successfully."
}

function post_test() {
  echo "==== POST-TEST VERIFICATION PHASE ===="

  # Create a temporary verification container using docker-compose
  echo "Verifying results..."
  docker compose -f docker-compose-spark.yml run --rm \
    -v $(pwd)/sql:/app/sql \
    spark-runner bash -c '
    /opt/spark/bin/spark-sql --master "local[1]" \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
      -f /app/sql/verify-results.sql
  '

  if [ $? -ne 0 ]; then
    echo "Verification failed!"
    exit 1
  fi

  echo "Verification completed successfully."
}

function cleanup() {
  echo "==== CLEANUP PHASE ===="
  echo "Cleaning up resources..."
  docker compose -f docker-compose-spark.yml down
  echo "Cleanup completed."
}

# Main execution
echo "Starting Vehicle ETL Integration Test..."
pre_test
run_etl
post_test
cleanup
echo "Integration test completed successfully!"