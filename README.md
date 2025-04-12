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
⭕ John (Customer #1)
      /     \
     /       \
    /         \
PURCHASED    PURCHASED
2023-01-15   2023-02-01
$1200        $300
   |          |
   |          |
   v          v
  ⭕         ⭕
Laptop      Headphones
(#101)      (#102)
   |          |
   |          |
PURCHASED    PURCHASED
2023-01-20   2023-03-01
$1200        $300
   |          |
   |          |
   v          v
   ⭕ Alice (Customer #2)
   
   ⭕ Bob (Customer #3)
    \
     \
      \
    PURCHASED
    2023-02-15
    $800
      \
       \
        \
         v
         ⭕
       Tablet
       (#103)
```

### Vertices (Nodes)
- **Customer vertices**: Contain customer data such as ID, name, location, age
- **Product vertices**: Contain product data such as ID, name, category, price

### Edges (Relationships)
- **PURCHASED relationship**: Links customers to products they've purchased, with attributes like purchase date, quantity, and amount

## Analytics Capabilities and Insights

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

From this sample data, we can extract several valuable insights:

1. **Customer Communities**: John and Alice form a distinct purchasing community separate from Bob
2. **Product Centrality**: The Laptop has the highest influence score, indicating it's a central product
3. **Customer Influence**: John and Alice have equal influence in the network
4. **Product Relationships**: Customers who buy Laptops also tend to buy Headphones
5. **Isolated Segments**: Bob and his Tablet purchase represent an isolated segment that may benefit from targeted marketing

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
```

Pipeline Manager
Background
Pipeline Manager is a framework designed to streamline the creation, management, and execution of ETL (Extract, Transform, Load) pipelines using Apache Airflow and Apache Spark. It allows data engineers to define data pipelines in simple YAML configurations and handles the complexities of deployment and execution.
Strategic Value

Simplified Pipeline Management: Abstract away the complexities of Airflow DAG definition and Docker configuration
Standardization: Enforce consistent patterns across all data pipelines
Self-contained Pipelines: Each pipeline includes its own code, configurations, SQL scripts, and data
Reproducibility: Pipeline definitions are version-controlled and can be easily deployed across environments
Developer Experience: Reduce boilerplate and allow data engineers to focus on business logic rather than infrastructure

Architecture
The Pipeline Manager consists of:

A YAML-based pipeline definition format
A CLI tool to generate DAGs from YAML and set up execution environments
Docker containers for Airflow, Spark, and Hive Metastore
A consistent folder structure for organizing pipeline assets

Getting Started
Prerequisites

Docker and Docker Compose
Python 3.8+
Access to the required JAR files for your ETL jobs

Pipeline Structure
Each pipeline should follow this structure:

```
pipelines/
└── your_pipeline/
├── your_pipeline.yml    # Pipeline definition
├── sql/                 # SQL initialization scripts
│   └── init-tables.sql
├── data/                # Sample or test data
│   └── sample.csv
└── jar/                 # JAR files for Spark jobs
└── your-etl.jar
```

Operations Guide
Creating a New Pipeline

Create a directory structure as shown above
Define your pipeline in YAML format
Add SQL scripts to initialize required tables
Add your JAR files containing ETL logic

Generate a DAG from YAML

```
python -m pipeline_manager.cli generate pipelines/your_pipeline
```

This will create a .dag file from your YAML definition.
Set Up and Run a Pipeline

```
python -m pipeline_manager.cli setup pipelines/your_pipeline
```

This will:

Copy your DAG file to the Airflow dags directory
Start the Docker environment (Hive Metastore, Airflow, etc.)
Run SQL initialization scripts
Make your JAR files available to the environment

Access the Airflow UI
After running the setup command, access the Airflow UI at:

URL: http://localhost:8081
Username: admin
Password: admin

From here, you can manually trigger your DAG to execute the pipeline.
Docker Commands

View running containers:

```
docker compose -f docker/docker-compose-airflow-spark.yml ps
```
View logs from a specific service:

```
docker compose -f docker/docker-compose-airflow-spark.yml logs airflow-webserver
```
Execute a command in the Spark container:

```
docker exec -it spark-runner bash
```
Shut down the environment:

```
docker compose -f docker/docker-compose-airflow-spark.yml down
```

Troubleshooting

If you encounter port conflicts, edit the docker/docker-compose-airflow-spark.yml file to change the port mappings.
For issues with the SQL execution, check the Spark container logs.
If DAGs fail, check the Airflow task logs in the UI for detailed error messages.

Examples
See the pipelines/vehicle_etl directory for a complete example pipeline that demonstrates how to:

Define a pipeline using YAML
Initialize database tables using SQL
Process data using Spark
