package com.insite.etl.vehicle

import com.insite.etl.common.utils.{SparkUtils, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.junit.{After, Assert, Before, Test}

/**
 * Test class for VehicleETL with versioned partitioning support
 */
class VehicleETLTest {

  implicit var spark: SparkSession = _
  implicit var properties: Map[String, String] = _
  var testData: DataFrame = _
  val testBasePath = "file:///tmp/test_vehicle_data" // Ensure three slashes for file: protocol
  val testVersion = "v001"

  @Before
  def setup(): Unit = {
    // Create test Spark session
    spark = TestUtils.createTestSparkSession()

    // Set up test properties with partition columns
    properties = Map(
      "source-table" -> "test_db.vehicles",
      "target-table" -> "test_db.processed_vehicles",
      "base-path" -> testBasePath,
      "version" -> testVersion,
      "partition-columns" -> "date,hour"
    )

    // Load test data
    testData = TestUtils.loadTestData(
      spark,
      "data/test_vehicles.json",
      "json",
      Map("multiline" -> "false")
    )

    // Create test table
    TestUtils.setupTestTable(spark, testData, "test_db.vehicles")

    // Create the target table schema in advance (as we expect in production)
    // This simulates the table being created separately from the ETL
    val createTableSQL = s"""
      CREATE TABLE IF NOT EXISTS test_db.processed_vehicles (
        vin STRING,
        manufacturer STRING,
        year INT,
        model STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        timestamp BIGINT,
        velocity INT,
        frontLeftDoorState STRING,
        wipersState BOOLEAN,
        gearPosition STRING,
        driverSeatbeltState STRING,
        date STRING,
        hour INT
      )
      USING PARQUET
      PARTITIONED BY (date, hour)
      LOCATION '$testBasePath/v000'
    """
    spark.sql(createTableSQL)

    println("Test environment initialized with partitioned table")
  }

  @Test
  def testDoLogic(): Unit = {
    // Create ETL instance
    val etl = new VehicleETL()

    // Execute the logic
    val result = etl.doLogic()

    // Verify results
    // 1. Check row count - should be less than original due to null VIN filtering
    val originalCount = testData.count()
    val resultCount = result.count()
    Assert.assertTrue("Result should have fewer rows due to null VIN filtering", resultCount < originalCount)

    // 2. Check manufacturer trimming
    val untrimmedCount = testData.filter(trim(col("manufacturer")) =!= col("manufacturer")).count()
    val trimmedCount = result.filter(trim(col("manufacturer")) =!= col("manufacturer")).count()
    Assert.assertEquals("All manufacturers should be trimmed", 0, trimmedCount)
    Assert.assertTrue("Original data should have untrimmed manufacturers", untrimmedCount > 0)

    // 3. Check gear position standardization
    val nonNumericGearCount = result.filter(!col("gearPosition").rlike("^-?[0-9]+$")).count()
    Assert.assertEquals("All gear positions should be numeric", 0, nonNumericGearCount)

    // 4. Check partition columns are created
    Assert.assertTrue("Date column should exist", result.columns.contains("date"))
    Assert.assertTrue("Hour column should exist", result.columns.contains("hour"))

    println("doLogic test passed")
  }

  @Test
  def testDoOutput(): Unit = {
    // Create ETL instance
    val etl = new VehicleETL()

    // Execute the logic
    val processedData = etl.doLogic()
    val processedCount = processedData.count()

    // Execute output to write partitioned data
    etl.doOutput(processedData)

    // Verify the output table exists and has the correct partitioning
    val outputData = spark.table("test_db.processed_vehicles")
    Assert.assertNotNull("Output table should exist", outputData)

    // Check the table location - IMPORTANT: Use proper path format checking
    val tableLocation = spark.sql("DESCRIBE FORMATTED test_db.processed_vehicles")
      .filter(col("col_name") === "Location")
      .select("data_type")
      .first()
      .getString(0)
      .trim

    // Standardize the path format before comparison to handle different file:/ formats
    val expectedPath = s"$testBasePath/$testVersion".replaceAll("file:/+", "file:///")
    val actualPath = tableLocation.replaceAll("file:/+", "file:///")

    Assert.assertEquals("Table location should point to the latest version", expectedPath, actualPath)

    // Check that partitions are registered
    val partitions = spark.sql("SHOW PARTITIONS test_db.processed_vehicles").collect()

    // Check we have at least one partition registered
    Assert.assertTrue("Table should have partitions", partitions.length > 0)

    // Verify data can be queried through the table
    val countFromTable = spark.table("test_db.processed_vehicles").count()
    Assert.assertEquals("All processed data should be accessible via table", processedCount, countFromTable)

    println("doOutput test passed")
  }

  @After
  def tearDown(): Unit = {
    // Clean up test tables
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS test_db.vehicles")
      spark.sql("DROP TABLE IF EXISTS test_db.processed_vehicles")
      TestUtils.cleanupTestEnv(spark)
    }
  }
}