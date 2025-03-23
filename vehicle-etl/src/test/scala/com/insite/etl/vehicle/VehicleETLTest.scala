package com.insite.etl.vehicle

import com.insite.etl.common.utils.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.junit.{After, Assert, Before, Test}

/**
 * Test class for VehicleETL
 */
class VehicleETLTest {

  implicit var spark: SparkSession = _
  implicit var properties: Map[String, String] = _
  var testData: DataFrame = _

  @Before
  def setup(): Unit = {
    // Create test Spark session
    spark = TestUtils.createTestSparkSession()

    // Set up test properties
    properties = Map(
      "source-table" -> "test_db.vehicles",
      "target-table" -> "test_db.processed_vehicles"
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

    println("Test environment initialized")
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

    println("doLogic test passed")
  }

  @Test
  def testDoOutput(): Unit = {
    // Create ETL instance
    val etl = new VehicleETL()

    // Execute the logic and output
    val processedData = etl.doLogic()
    etl.doOutput(processedData)

    // Verify the output table exists and has the expected data
    val outputData = spark.table("test_db.processed_vehicles")
    Assert.assertNotNull("Output table should exist", outputData)
    Assert.assertEquals("Output table should have the same number of rows as processed data",
      processedData.count(), outputData.count())

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