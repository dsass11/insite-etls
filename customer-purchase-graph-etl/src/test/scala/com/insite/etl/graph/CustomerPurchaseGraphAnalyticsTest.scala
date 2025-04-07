package com.insite.etl.graph

import com.insite.etl.common.utils.TestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

class CustomerPurchaseGraphAnalyticsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  // Create a test Spark session
  var spark: SparkSession = _

  // Declare properties at class level
  implicit var properties: Map[String, String] = _

  override def beforeAll(): Unit = {
    spark = TestUtils.createTestSparkSession()

    // Initialize properties
    properties = Map.empty[String, String]

    // Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS retail")
    spark.sql("USE retail")
  }

  test("Graph analytics should identify important nodes and communities") {
    // Use import inside the test method, not at the class level
    val sparkSession = spark

    // Then use sparkSession for all operations
    import sparkSession.implicits._

    // Create test data
    val testCustomers = Seq(
      (1, "John", "New York", 32),
      (2, "Alice", "Boston", 28),
      (3, "Bob", "Chicago", 45)
    ).toDF("customer_id", "name", "location", "age")

    val testProducts = Seq(
      (101, "Laptop", "Electronics", 1200.0),
      (102, "Headphones", "Electronics", 300.0),
      (103, "Tablet", "Electronics", 800.0)
    ).toDF("product_id", "name", "category", "price")

    val testPurchases = Seq(
      (1001, 1, 101, "2023-01-15", 1, 1200.0),
      (1002, 2, 101, "2023-01-20", 1, 1200.0),
      (1003, 1, 102, "2023-02-01", 1, 300.0),
      (1004, 3, 103, "2023-02-15", 1, 800.0),
      (1005, 2, 102, "2023-03-01", 1, 300.0)
    ).toDF("purchase_id", "customer_id", "product_id", "purchase_date", "quantity", "amount")

    // Register tables
    testCustomers.createOrReplaceTempView("customers")
    testProducts.createOrReplaceTempView("products")
    testPurchases.createOrReplaceTempView("purchases")

    // Create empty tables for new data
    // Fix these lines that were causing errors
    val emptyDF1 = spark.emptyDataFrame
    val emptyDF2 = spark.emptyDataFrame
    val emptyDF3 = spark.emptyDataFrame

    emptyDF1.createOrReplaceTempView("new_customers")
    emptyDF2.createOrReplaceTempView("new_products")
    emptyDF3.createOrReplaceTempView("new_purchases")

    // Run the ETL
    val etl = new CustomerPurchaseGraphETL()
    val result = etl.doLogic()(spark, properties)

    // Split result into vertices and edges
    val splitMethod = etl.getClass.getDeclaredMethod("splitOutputDataset", classOf[DataFrame])
    splitMethod.setAccessible(true)
    val (vertices, edges) = splitMethod.invoke(etl, result).asInstanceOf[(DataFrame, DataFrame)]

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
    influentialCustomers.count() should be <= testCustomers.count()
    influentialCustomers.filter($"vertex_type" === "customer").count() should be (influentialCustomers.count())

    // Test popular products
    val popularProductsMethod = etl.getClass.getDeclaredMethod("findPopularProducts", classOf[GraphFrame], classOf[Int])
    popularProductsMethod.setAccessible(true)
    val popularProducts = popularProductsMethod.invoke(etl, graph, Int.box(10)).asInstanceOf[DataFrame]

    // Verify popular products
    popularProducts.count() should be <= testProducts.count()
    popularProducts.filter($"vertex_type" === "product").count() should be (popularProducts.count())
  }

  override def afterAll(): Unit = {
    // Clean up
    if (spark != null) {
      spark.sql("DROP DATABASE IF EXISTS retail CASCADE")
      spark.stop()
    }
  }
}