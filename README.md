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

Integration Testing
-------------------

This project includes a robust integration testing framework for validating ETL processes with special focus on Scala 2.11 compatibility with Spark 2.4.8. The integration tests ensure proper execution of ETL jobs in a containerized environment.

### Key Features

*   **Containerized Testing Environment**: Docker-based setup ensures consistent, reproducible tests
    
*   **Scala 2.11 and Spark 2.4.8 Support**: Custom container configuration specifically tuned for Scala 2.11
    
*   **Hive Metastore Integration**: Tests validate proper interaction with Hive tables
    
*   **Modular Structure**: Separate SQL, data, and configuration components for maintainability
    

### Challenges Addressed

*   **Scala Version Compatibility**: Resolved the common NoSuchMethodError: scala.Predef$.refArrayOps error that occurs when trying to run Scala 2.11 code in a Scala 2.12 environment
    
*   **Hive Table Partitioning**: Implemented proper table partitioning and registration
    
*   **Data Persistence**: Ensured data persists correctly between test phases
    
*   **Test Automation**: Created a structured testing approach with pre-test, execution, verification, and cleanup phases
    

### Test Structure

The integration test framework follows a four-phase approach:

1.  **Pre-Test Phase**: Initializes the environment and starts necessary services
    
2.  **ETL Execution Phase**: Loads test data, creates tables, and runs the ETL job
    
3.  **Verification Phase**: Validates the ETL job's output using SQL queries
    
4.  **Cleanup Phase**: Tears down the environment and cleans up resources
    

### Implementation Details

*   **Docker Compose**: Uses Docker Compose to orchestrate containers
    
*   **External Files**: Stores SQL queries and test data in separate files for clarity
    
*   **Consistent Environment**: Ensures the ETL job runs in the same environment every time
    
*   **Error Handling**: Captures and reports errors with appropriate exit codes
    

### Example Usage

To run the integration tests:

mvn clean install ## to create the jar etl file

cd integration-tests/vehicle-etl-integration-test ## move to itegration etl folder test

cp ../../vehicle-etl/target/vehicle-etl-1.0-SNAPSHOT.jar ./vehicle-etl.jar ## copy the created jar into integration test location

./run-etl-test.sh ## run the integration test   `

### Project Directory Structure

```plaintext
integration-tests/
└── vehicle-etl-integration-test/
    ├── README.md                # Test-specific documentation
    ├── Dockerfile.spark         # Custom Spark 2.4.8 with Scala 2.11
    ├── docker-compose-spark.yml # Docker Compose configuration
    ├── run-etl-test.sh          # Main test execution script
    ├── data/                    # Test data
    │   └── vehicles.csv         # Sample vehicle data
    └── sql/                     # SQL scripts for test phases
        ├── init-tables.sql      # Table creation and initialization
        └── verify-results.sql   # Result verification queries
```


### Lessons Learned

*   **Scala Compatibility**: When working with Spark, ensure Scala version compatibility between your code and environment
    
*   **Docker Networking**: Use Docker Compose networks to ensure services can communicate properly
    
*   **Hive Metastore**: Configure Hive metastore connection string correctly for stable integration
    
*   **File Paths**: Be careful with file paths inside containers vs. host machine
    
*   **Error Diagnostics**: Add diagnostic commands to help troubleshoot failed tests
    

The integration tests serve not only as validation tools but also as documentation of the expected behavior of the ETL processes.

## ETL Metrics Framework

The framework includes a comprehensive metrics collection system to monitor and analyze ETL job performance and data quality.

### Key Features

- **Automatic Metrics Collection**: Technical metrics like execution time, record counts, and job status are collected automatically
- **Business Metrics Support**: ETL implementations can add domain-specific metrics relevant to their data
- **Separation of Concerns**: Core ETL logic remains clean while metrics are collected non-intrusively
- **Extensible Reporting**: Metrics can be reported to various destinations (console, databases, monitoring systems)

### Technical Metrics

The framework automatically collects the following technical metrics:

- **Job Information**: Job ID, ETL class name, execution timestamp
- **Execution Timing**: Duration of doLogic and doOutput phases
- **Status Tracking**: SUCCESS, FAILED, or RUNNING status
- **Record Counts**: Number of records processed
- **Error Details**: When failures occur, error information is captured

### Business Metrics

ETL implementations can provide custom business metrics by overriding the `collectBusinessMetrics` method:

```scala
override def collectBusinessMetrics(data: DataFrame, metrics: ETLMetrics)
                                (implicit spark: SparkSession, properties: Map[String, String]): ETLMetrics = {
  // Add business-specific metrics
  metrics.addBusinessMetric("metric_name", value)
}
```
### Usage 
Metrics are automatically collected during ETL execution. You can specify a custom metrics reporter:
```aidl
spark-submit --class com.insite.etl.common.ETLRunner \
  insite-etls-1.0-SNAPSHOT.jar \
  com.insite.etl.vehicle.VehicleETL \
  metrics-reporter=console \
  metrics-name="Vehicle Processing Job"
```

# Customer Purchase Graph ETL

A Spark ETL module that analyzes retail customer purchase patterns using graph analytics.

## Overview

This module builds a graph representation of customer purchase data, where:
- Customers and products are represented as vertices (nodes)
- Purchase transactions are represented as edges (relationships)

The resulting graph enables powerful analytics to identify influential customers, popular products, and purchase communities.

## Graph Structure
```
      (Customer 1)
      /          \
     /            \
    /              \
PURCHASED        PURCHASED
    \              /
     \            /
      v          v
(Product B)    (Product A)
      \          /
       \        /
        \      /
       PURCHASED
          |
          |
          v
     (Customer 2)
```

### Vertices (Nodes)
- **Customer vertices**: Contain customer data such as ID, name, location, age
- **Product vertices**: Contain product data such as ID, name, category, price

### Edges (Relationships)
- **PURCHASED relationship**: Links customers to products they've purchased, with attributes like purchase date, quantity, and amount

## Analytics Capabilities

This module leverages GraphFrames to perform advanced graph analytics:

1. **PageRank Analysis**: Identifies influential nodes in the purchase network
   - High-PageRank customers may be trend-setters or influencers
   - High-PageRank products may be gateway purchases leading to other purchases

2. **Connected Components**: Identifies distinct purchasing communities
   - Helps segment customers by purchasing patterns
   - Reveals product clusters that are frequently purchased together

3. **Customer Influence Metrics**: Ranks customers by their influence in the network
   - Identifies high-value customers for marketing campaigns
   - Can help predict product adoption trends

4. **Product Popularity Analysis**: Ranks products by their centrality in the network
   - Goes beyond simple purchase counts to understand product relationships
   - Helps identify products that drive additional purchases

## How It Works

1. **Data Ingestion**: Reads from existing and new customer, product, and purchase data
2. **Graph Construction**: Builds vertices for customers/products and edges for purchases
3. **Analytics Processing**: Runs graph algorithms to extract insights
4. **Output**: Writes processed graph data to tables for reporting and visualization

## Testing

The analytics tests demonstrate:
- Graph construction from retail data
- PageRank calculation for identifying important nodes
- Community detection through connected components
- Identification of influential customers and popular products

## Example Usage

```scala
// Initialize with configuration
val etl = new CustomerPurchaseGraphETL()

// Process the graph data
val result = etl.doLogic()

// Write results to output tables
etl.doOutput(result)

// Access analytics results
val metrics = etl.collectBusinessMetrics(result, initialMetrics)