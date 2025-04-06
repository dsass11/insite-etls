package com.insite.etl.common

import com.insite.etl.common.utils.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Example ScalaTest style test using standard TestUtils
 * This class demonstrates the recommended approach for ScalaTest testing with Spark
 * utilizing the expressive "should" syntax
 */
class ScalaTestStyleExampleTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // Declare SparkSession as an instance variable so it can be used across methods
  implicit val spark: SparkSession = TestUtils.createTestSparkSession(appName = "ScalaTestExample")

  // Test configuration properties - these would be specific to your ETL
  implicit val properties: Map[String, String] = Map(
    "source-table" -> "test_db.source",
    "target-table" -> "test_db.target",
    "base-path" -> "file:///tmp/test_scalatest_example",
    "version" -> "v001"
  )

  // Create test data
  val testData: DataFrame = {
    // Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("USE test_db")

    // Create test data using TestUtils helper method
    val data = TestUtils.createTestDataFrame(
      spark,
      Seq(
        Seq(1, "Product A", 10.5, "2025-01-01"),
        Seq(2, "Product B", 20.0, "2025-01-02"),
        Seq(3, "Product C", 15.75, "2025-01-03")
      ),
      Seq("id", "name", "price", "date")
    )

    // Set up test table
    TestUtils.setupTestTable(spark, data, "test_db.source")

    data
  }

  /**
   * Example test that demonstrates data transformations with expressive assertions
   */
  test("Data transformation should correctly categorize prices") {
    // Import implicits inside the test method
    import spark.implicits._

    // Read the source data
    val sourceData = spark.table("test_db.source")

    // Apply a transformation
    val transformedData = sourceData
      .withColumn("price_category",
        when(col("price") < 15, "Low")
          .when(col("price") < 20, "Medium")
          .otherwise("High"))

    // Write to target table
    transformedData.write
      .mode("overwrite")
      .saveAsTable("test_db.target")

    // Verify the result using expressive "should" syntax
    val resultData = spark.table("test_db.target")

    // Check row count with expressive assertion
    resultData.count() shouldBe 3

    // Check price categories using ScalaTest's expressive assertions
    val lowPriced = resultData.filter($"price_category" === "Low").collect()
    lowPriced should have length 1
    lowPriced.head.getAs[String]("name") should be ("Product A")

    val mediumPriced = resultData.filter($"price_category" === "Medium").collect()
    mediumPriced should have length 1
    mediumPriced.head.getAs[String]("name") should be ("Product C")

    val highPriced = resultData.filter($"price_category" === "High").collect()
    highPriced should have length 1
    highPriced.head.getAs[String]("name") should be ("Product B")

    // Collection-based assertions
    val priceCategories = resultData.select("price_category").distinct().collect().map(_.getString(0))
    priceCategories should contain allOf ("Low", "Medium", "High")
    priceCategories should have length 3
  }

  /**
   * Example test demonstrating DataFrame comparisons with should style
   */
  test("DataFrame comparison should correctly identify matching DataFrames") {
    // Create expected DataFrame
    val expectedData = TestUtils.createTestDataFrame(
      spark,
      Seq(
        Seq(1, "Product A", 10.5, "Low"),
        Seq(2, "Product B", 20.0, "High"),
        Seq(3, "Product C", 15.75, "Medium")
      ),
      Seq("id", "name", "price", "price_category")
    )

    // Create actual DataFrame with transformation
    val actualData = testData
      .withColumn("price_category",
        when(col("price") < 15, "Low")
          .when(col("price") < 20, "Medium")
          .otherwise("High"))
      .select("id", "name", "price", "price_category")

    // Compare DataFrames using TestUtils
    val dataFramesEqual = TestUtils.dataFramesEqual(actualData, expectedData)
    dataFramesEqual shouldBe true

    // Additional assertions on the DataFrame
    actualData.columns should contain allOf ("id", "name", "price", "price_category")
    actualData.columns should have length 4
  }

  /**
   * Test validation with rich assertions on DataFrame content
   */
  test("Data transformation results should match expected values with decimal precision") {
    // Apply a transformation
    val transformedData = testData
      .withColumn("discount", col("price") * 0.1)
      .withColumn("final_price", col("price") - col("discount"))

    // Validate specific rows with rich assertions
    val productA = transformedData.filter(col("name") === "Product A").collect()(0)

    // Use approximate equality for floating point
    productA.getAs[Double]("discount") shouldBe 1.05 +- 0.001
    productA.getAs[Double]("final_price") shouldBe 9.45 +- 0.001

    // Check different products with collection matchers
    val productPrices = transformedData.select("name", "final_price")
      .collect()
      .map(row => (row.getString(0), row.getDouble(1)))
      .toMap

    productPrices.keys should contain allOf ("Product A", "Product B", "Product C")
    productPrices("Product B") shouldBe 18.0 +- 0.001

    // Using ordering matchers
    val orderedPrices = transformedData.select("final_price")
      .collect()
      .map(_.getDouble(0))
      .sorted

    orderedPrices.length shouldBe 3
    orderedPrices(0) shouldBe 9.45 +- 0.001
    orderedPrices(1) shouldBe 14.175 +- 0.001
    orderedPrices(2) shouldBe 18.0 +- 0.001

    // String matchers
    val productNames = transformedData.select("name")
      .collect()
      .map(_.getString(0))

    productNames should contain allElementsOf Seq("Product A", "Product B", "Product C")
    productNames.mkString(",") should include ("Product")
    productNames should contain oneElementOf Seq("Product A", "Something Else")
  }

  /**
   * Clean up after all tests have run
   */
  override def afterAll(): Unit = {
    // Clean up test tables
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    TestUtils.cleanupTestEnv(spark)
  }
}