# Insite ETL Framework

A modular, extensible ETL (Extract, Transform, Load) framework for processing data with Apache Spark.

## Project Structure

The project follows a modular structure with a parent project and submodules:

- **insite-etls** (parent): Coordinates the build and provides common dependencies
- **common**: Core framework components and utilities shared across all ETL modules
- **vehicle-etl**: Implementation for vehicle data processing

## Core Concepts

### ETL Interface

The core of the framework is the `ETL` trait that defines the contract for all ETL implementations:

```scala
trait ETL {
  // Process the data and apply transformations
  def doLogic()(implicit spark: SparkSession, properties: Map[String, String]): DataFrame
  
  // Output the processed data to the destination
  def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit
}
```

### Separation of Logic and Output

ETL operations are separated into two distinct phases:

1. **doLogic**: Handles data acquisition and transformation
    - Reads from source tables/files
    - Applies business rules and transformations
    - Returns a processed DataFrame

2. **doOutput**: Handles data persistence with versioning
    - Writes data to a versioned directory
    - Updates table metadata to point to the latest version
    - Adds partition specifications to the table

### Versioned Partitioning

The framework supports a versioned approach to data storage:

- Each ETL run writes to a new version/timestamp directory
- Table metadata is updated to point to the latest version
- Historical data is preserved for auditing and rollback
- Partitioning is supported for optimized querying

## Utilities

### SparkUtils

Provides common functionality for Spark operations:

- Session management: `createSparkSession()`
- SQL execution: `executeSql()`
- Partitioned data handling: `writePartitioned()`, `updatePartitionedTableMetadata()`
- Accumulator approach: Use Spark accumulators to collect partition values during the initial DataFrame processing
Each executor would add partition values to shared accumulators as it processes records
No need to rescan the DataFrame after writing
### TestUtils

Facilitates testing ETL implementations:

- Test environment setup: `createTestSparkSession()`
- Test data loading: `loadTestData()`
- Test cleanup: `cleanupTestEnv()`

## Example: Vehicle ETL

The Vehicle ETL module demonstrates implementation of the framework:

```scala
class VehicleETL extends ETL {
  override def doLogic()(implicit spark: SparkSession, properties: Map[String, String]): DataFrame = {
    // Read source data
    val sourceTable = properties.getOrElse("source-table", "vehicle_datalake.events")
    val eventsDF = spark.sql(s"SELECT * FROM $sourceTable")
    
    // Apply transformations
    val processedDF = eventsDF
      .withColumn("manufacturer", trim(col("manufacturer")))
      .filter(col("vin").isNotNull)
      .withColumn("gearPosition", /* standardization logic */)
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))
      
    processedDF
  }
  
  override def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit = {
    // Get configuration
    val targetTable = properties.getOrElse("target-table", "vehicle_datalake.processed_events")
    val basePath = properties.getOrElse("base-path", "s3://my-bucket/data")
    val timestamp = System.currentTimeMillis().toString
    val version = properties.getOrElse("version", s"v$timestamp")
    
    // Handle partitioning
    val partitionCols = properties.get("partition-columns").map(_.split(",").map(_.trim))
    
    partitionCols match {
      case None | Some(Array()) => 
        throw new IllegalArgumentException("Partition columns must be specified")
        
      case Some(columns) => 
        // Write data with versioning and partitioning
        val partitionValues = SparkUtils.writePartitioned(data, basePath, version, columns)
        
        // Update table metadata to point to the latest version
        SparkUtils.updatePartitionedTableMetadata(targetTable, columns, partitionValues, basePath, version)
    }
  }
}
```

## Testing Approach

The framework includes a comprehensive testing approach:

1. **Test Environment Setup**
    - Create a test Spark session
    - Load test data
    - Set up test tables

2. **Test Data Quality**
    - Verify filtering rules
    - Check data cleaning operations
    - Validate data transformations

3. **Test Partitioning**
    - Verify partition columns are created
    - Check partition metadata registration
    - Validate version-based table location updates

4. **Test End-to-End Flow**
    - Verify data can be queried through the table
    - Validate record counts match between processed data and table

## Usage

To run an ETL job:

```bash
spark-submit --class com.insite.etl.common.ETLRunner \
  insite-etls-1.0-SNAPSHOT.jar \
  com.insite.etl.vehicle.VehicleETL \
  source-table=vehicle_data.events \
  target-table=vehicle_data.processed_events \
  base-path=s3://my-bucket/vehicle-data \
  partition-columns=date,hour
```

## Configuration Parameters

Common configuration parameters include:

- **source-table**: Source table name
- **target-table**: Target table name
- **base-path**: Base path for data storage
- **version**: Version identifier (defaults to timestamp)
- **partition-columns**: Comma-separated list of partition columns