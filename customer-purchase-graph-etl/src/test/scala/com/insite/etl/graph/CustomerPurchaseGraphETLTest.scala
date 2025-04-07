package com.insite.etl.graph

import com.insite.etl.common.utils.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CustomerPurchaseGraphETLTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  // Declare variables
  implicit var spark: SparkSession = _
  implicit var properties: Map[String, String] = _
  val testBasePath = "file:///tmp/test_graph_data"
  val testVersion = "v001"

  override def beforeAll(): Unit = {
    // Create test Spark session using TestUtils
    spark = TestUtils.createTestSparkSession()

    // Initialize properties
    properties = Map(
      "customers-table" -> "retail.customers",
      "products-table" -> "retail.products",
      "purchases-table" -> "retail.purchases",
      "new-customers-table" -> "retail.new_customers",
      "new-products-table" -> "retail.new_products",
      "new-purchases-table" -> "retail.new_purchases",
      "target-vertices-table" -> "retail.processed_vertices",
      "target-edges-table" -> "retail.processed_edges",
      "base-path" -> testBasePath,
      "version" -> testVersion
    )

    // Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS retail")
    spark.sql("USE retail")

    // Create test data directly (without implicits)
    val testCustomers = spark.createDataFrame(
      Seq(
        (1, "John", "New York", 32),
        (2, "Alice", "Boston", 28),
        (3, "Bob", "Chicago", 45)
      )
    ).toDF("customer_id", "name", "location", "age")

    val testProducts = spark.createDataFrame(
      Seq(
        (101, "Laptop", "Electronics", 1200.0),
        (102, "Headphones", "Electronics", 300.0),
        (103, "Tablet", "Electronics", 800.0)
      )
    ).toDF("product_id", "name", "category", "price")

    val testPurchases = spark.createDataFrame(
      Seq(
        (1001, 1, 101, "2023-01-15", 1, 1200.0),
        (1002, 2, 101, "2023-01-20", 1, 1200.0),
        (1003, 1, 102, "2023-02-01", 1, 300.0),
        (1004, 3, 103, "2023-02-15", 1, 800.0),
        (1005, 2, 102, "2023-03-01", 1, 300.0)
      )
    ).toDF("purchase_id", "customer_id", "product_id", "purchase_date", "quantity", "amount")

    // Register tables using the TestUtils method
    TestUtils.setupTestTable(spark, testCustomers, "retail.customers")
    TestUtils.setupTestTable(spark, testProducts, "retail.products")
    TestUtils.setupTestTable(spark, testPurchases, "retail.purchases")

    // Create empty tables for new data
    val emptyCustomers = spark.createDataFrame(
      Seq.empty[(Int, String, String, Int)]
    ).toDF("customer_id", "name", "location", "age")

    val emptyProducts = spark.createDataFrame(
      Seq.empty[(Int, String, String, Double)]
    ).toDF("product_id", "name", "category", "price")

    val emptyPurchases = spark.createDataFrame(
      Seq.empty[(Int, Int, Int, String, Int, Double)]
    ).toDF("purchase_id", "customer_id", "product_id", "purchase_date", "quantity", "amount")

    TestUtils.setupTestTable(spark, emptyCustomers, "retail.new_customers")
    TestUtils.setupTestTable(spark, emptyProducts, "retail.new_products")
    TestUtils.setupTestTable(spark, emptyPurchases, "retail.new_purchases")
  }

  test("Graph analytics should work correctly") {
    // Test implementation from testGraphAnalytics
    // Run the ETL
    val etl = new CustomerPurchaseGraphETL()
    val result = etl.doLogic()

    // Split result into vertices and edges
    val splitMethod = etl.getClass.getDeclaredMethod("splitOutputDataset", classOf[DataFrame])
    splitMethod.setAccessible(true)
    val splitResult = splitMethod.invoke(etl, result).asInstanceOf[(DataFrame, DataFrame)]
    val (vertices, edges) = splitResult

    // Create GraphFrame
    val createGraphFrameMethod = etl.getClass.getDeclaredMethod("createGraphFrame", classOf[DataFrame], classOf[DataFrame])
    createGraphFrameMethod.setAccessible(true)
    val graph = createGraphFrameMethod.invoke(etl, vertices, edges).asInstanceOf[GraphFrame]

    // Test PageRank
    val pageRankMethod = etl.getClass.getDeclaredMethod("runPageRank", classOf[GraphFrame], classOf[Int], classOf[Double])
    pageRankMethod.setAccessible(true)
    val pageRankResults = pageRankMethod.invoke(etl, graph, Int.box(10), Double.box(0.15)).asInstanceOf[DataFrame]

    // Verify PageRank
    pageRankResults.count() should be (vertices.count())
    pageRankResults.columns should contain ("pagerank")

    // Test connected components
    val componentsMethod = etl.getClass.getDeclaredMethod("findConnectedComponents", classOf[GraphFrame])
    componentsMethod.setAccessible(true)
    val componentsResults = componentsMethod.invoke(etl, graph).asInstanceOf[DataFrame]

    // Verify components
    componentsResults.count() should be (vertices.count())
    componentsResults.columns should contain ("component")

    // Test influentialCustomers
    val influentialCustomersMethod = etl.getClass.getDeclaredMethod("findInfluentialCustomers", classOf[GraphFrame], classOf[Int])
    influentialCustomersMethod.setAccessible(true)
    val influentialCustomers = influentialCustomersMethod.invoke(etl, graph, Int.box(10)).asInstanceOf[DataFrame]

    // Verify influential customers
    influentialCustomers.count() should be > 0L
    influentialCustomers.filter(col("vertex_type") === "customer").count() should be (influentialCustomers.count())

    // Test popular products
    val popularProductsMethod = etl.getClass.getDeclaredMethod("findPopularProducts", classOf[GraphFrame], classOf[Int])
    popularProductsMethod.setAccessible(true)
    val popularProducts = popularProductsMethod.invoke(etl, graph, Int.box(10)).asInstanceOf[DataFrame]

    // Verify popular products
    popularProducts.count() should be > 0L
    popularProducts.filter(col("vertex_type") === "product").count() should be (popularProducts.count())
  }

  test("ETL Output should work correctly") {
    // Implementation from testEtlOutput
    // Run the ETL
    val etl = new CustomerPurchaseGraphETL()
    val processedData = etl.doLogic()

    // Execute the output method
    etl.doOutput(processedData)

    // Verify the output tables exist
    spark.catalog.tableExists("retail.processed_vertices") should be (true)
    spark.catalog.tableExists("retail.processed_edges") should be (true)

    // Verify data in the tables
    val verticesData = spark.table("retail.processed_vertices")
    val edgesData = spark.table("retail.processed_edges")

    verticesData.count() should be > 0L
    edgesData.count() should be > 0L
  }

  override def afterAll(): Unit = {
    // Clean up
    if (spark != null) {
      spark.sql("DROP DATABASE IF EXISTS retail CASCADE")
      TestUtils.cleanupTestEnv(spark)
      spark.stop()
    }
  }
}