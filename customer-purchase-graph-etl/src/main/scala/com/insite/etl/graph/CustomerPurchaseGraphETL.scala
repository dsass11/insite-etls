package com.insite.etl.graph

import com.insite.etl.common.ETL
import com.insite.etl.common.metrics.ETLMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

/**
 * Implementation of ETL for customer purchase graph analysis.
 * This ETL builds a graph of customers, products, and purchase relationships.
 */


class CustomerPurchaseGraphETL extends ETL {

  /**
   * Process the customer purchase graph data
   *
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   * @return DataFrame with processed data (combined vertex and edge data for output)
   */
  override def doLogic()(implicit spark: SparkSession, properties: Map[String, String]): DataFrame = {
    // Get source tables from properties
    val customersTable = properties.getOrElse("customers-table", "retail.customers")
    val productsTable = properties.getOrElse("products-table", "retail.products")
    val purchasesTable = properties.getOrElse("purchases-table", "retail.purchases")
    val newCustomersTable = properties.getOrElse("new-customers-table", "retail.new_customers")
    val newProductsTable = properties.getOrElse("new-products-table", "retail.new_products")
    val newPurchasesTable = properties.getOrElse("new-purchases-table", "retail.new_purchases")

    println(s"Reading from customers table: $customersTable")
    println(s"Reading from products table: $productsTable")
    println(s"Reading from purchases table: $purchasesTable")
    println(s"Reading new customers from: $newCustomersTable")
    println(s"Reading new products from: $newProductsTable")
    println(s"Reading new purchases from: $newPurchasesTable")

    // Read existing data
    val customersDF = spark.table(customersTable)
    val productsDF = spark.table(productsTable)
    val purchasesDF = spark.table(purchasesTable)

    // Read update data
    val newCustomersDF = spark.table(newCustomersTable)
    val newProductsDF = spark.table(newProductsTable)
    val newPurchasesDF = spark.table(newPurchasesTable)

    // Prepare vertices: combine customers and products, adding a type column
    val customerVerticesDF = customersDF
      .withColumn("vertex_type", lit("customer"))
      .withColumnRenamed("customer_id", "id")

    val productVerticesDF = productsDF
      .withColumn("vertex_type", lit("product"))
      .withColumnRenamed("product_id", "id")

    // Combine all vertices
    val (customerVerticesDFAllCols, productVerticesDFAllCols) =
      addMissingColumns(customerVerticesDF, productVerticesDF)
    val verticesDF = customerVerticesDFAllCols.unionByName(productVerticesDFAllCols)

    // Prepare new vertices
    val newCustomerVerticesDF = newCustomersDF
      .withColumn("vertex_type", lit("customer"))
      .withColumnRenamed("customer_id", "id")

    val newProductVerticesDF = newProductsDF
      .withColumn("vertex_type", lit("product"))
      .withColumnRenamed("product_id", "id")

    // Combine all new vertices
    val (newCustomerVerticesDFAllCols, newProductVerticesDFAllCols) =
      addMissingColumns(newCustomerVerticesDF, newProductVerticesDF)
    val newVerticesDF = newCustomerVerticesDFAllCols.unionByName(newProductVerticesDFAllCols)

    // Prepare edges from purchases
    val edgesDF = purchasesDF
      .withColumnRenamed("customer_id", "src")
      .withColumnRenamed("product_id", "dst")
      .withColumn("relationship", lit("PURCHASED"))

    val newEdgesDF = newPurchasesDF
      .withColumnRenamed("customer_id", "src")
      .withColumnRenamed("product_id", "dst")
      .withColumn("relationship", lit("PURCHASED"))

    // Merge vertices and edges
    val mergedVerticesDF = mergeVertices(verticesDF, newVerticesDF)
    val mergedEdgesDF = mergeEdges(edgesDF, newEdgesDF)

    // Create GraphFrame from merged vertex and edge DataFrames
    val graph = createGraphFrame(mergedVerticesDF, mergedEdgesDF)

    // Debug information
    println(s"Created graph with ${graph.vertices.count()} vertices and ${graph.edges.count()} edges")

    // Run PageRank to identify important vertices
    val pageRankResults = runPageRank(graph)

    // Log the top 5 nodes by PageRank
    println("Top 5 nodes by PageRank:")
    pageRankResults.show(5,false)

    // Find connected components
    val componentResults = findConnectedComponents(graph)

    // Log component groups
    println("Connected components in graph:")
    componentResults.show(10, false)

    // Find influential customers
    val influentialCustomers = findInfluentialCustomers(graph, 5)
    println("Top 5 influential customers:")
    influentialCustomers.show(false)

    // Find popular products
    val popularProducts = findPopularProducts(graph, 5)
    println("Top 5 popular products:")
    popularProducts.show(false)

    // Print some debug information
    println(s"Total vertices: ${mergedVerticesDF.count()}")
    println(s"Total edges: ${mergedEdgesDF.count()}")

    // Combine results for output
    val outputDF = createOutputDataset(mergedVerticesDF, mergedEdgesDF)

    outputDF
  }

  /**
   * Save the processed customer purchase graph data
   *
   * @param data       DataFrame to save
   * @param spark      Implicit SparkSession
   * @param properties Implicit configuration properties
   */
  override def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit = {
    // Get configuration properties with defaults
    val targetVerticesTable = properties.getOrElse("target-vertices-table", "retail.processed_vertices")
    val targetEdgesTable = properties.getOrElse("target-edges-table", "retail.processed_edges")
    val basePath = properties.getOrElse("base-path", "s3://my-bucket/retail-graph")
    val version = properties.getOrElse("version", s"v${System.currentTimeMillis()}")

    // Split the combined output back into components
    val (verticesDF, edgesDF) = splitOutputDataset(data)

    // Save vertices
    val verticesPath = s"$basePath/vertices/$version"
    println(s"Writing vertices to: $verticesPath")
    verticesDF.write.mode("overwrite").parquet(verticesPath)

    // Create or update vertices table
    spark.sql(s"CREATE TABLE IF NOT EXISTS $targetVerticesTable USING PARQUET LOCATION '$verticesPath'")
    spark.sql(s"ALTER TABLE $targetVerticesTable SET LOCATION '$verticesPath'")

    // Save edges
    val edgesPath = s"$basePath/edges/$version"
    println(s"Writing edges to: $edgesPath")
    edgesDF.write.mode("overwrite").parquet(edgesPath)

    // Create or update edges table
    spark.sql(s"CREATE TABLE IF NOT EXISTS $targetEdgesTable USING PARQUET LOCATION '$edgesPath'")
    spark.sql(s"ALTER TABLE $targetEdgesTable SET LOCATION '$edgesPath'")

    println(s"Successfully updated customer purchase graph tables")
  }

  /**
   * Collect business metrics from the graph data
   *
   * @param data DataFrame containing the processed data
   * @param metrics Current ETLMetrics object
   * @return Updated ETLMetrics with business metrics
   */
  override def collectBusinessMetrics(data: DataFrame, metrics: ETLMetrics)
                                     (implicit spark: SparkSession, properties: Map[String, String]): ETLMetrics = {
    import spark.implicits._

    // Split the output data to get vertices and edges
    val (vertices, edges) = splitOutputDataset(data)

    // Create GraphFrame for analysis
    val graph = createGraphFrame(vertices, edges)

    // Count customers by location
    val customersByLocation = graph.vertices
      .filter($"vertex_type" === "customer")
      .groupBy("location")
      .count()
      .collect()
      .map(row => (s"customers_in_${row.getAs[String]("location")}", row.getAs[Long]("count")))
      .toMap

    // Count products by category
    val productsByCategory = graph.vertices
      .filter($"vertex_type" === "product")
      .groupBy("category")
      .count()
      .collect()
      .map(row => (s"products_in_${row.getAs[String]("category")}", row.getAs[Long]("count")))
      .toMap

    // Get min and max purchase dates from edges
    val purchaseDates = edges
      .filter($"relationship" === "PURCHASED")
      .select($"purchase_date")
      .agg(
        min($"purchase_date").alias("min_date"),
        max($"purchase_date").alias("max_date")
      )
      .collect()
      .headOption
      .map(row => Map(
        "min_date" -> row.getAs[String]("min_date"),
        "max_date" -> row.getAs[String]("max_date")
      ))
      .getOrElse(Map.empty[String, String])

    // Combine all metrics
    var updatedMetrics = metrics

    // Add the total counts
    updatedMetrics = updatedMetrics.copy(
      businessMetrics = updatedMetrics.businessMetrics ++ Map(
        "total_vertices" -> vertices.count(),
        "total_edges" -> edges.count()
      )
    )

    // Add location metrics
    updatedMetrics = updatedMetrics.copy(
      businessMetrics = updatedMetrics.businessMetrics ++ customersByLocation
    )

    // Add category metrics
    updatedMetrics = updatedMetrics.copy(
      businessMetrics = updatedMetrics.businessMetrics ++ productsByCategory
    )

    // Add date metrics
    updatedMetrics = updatedMetrics.copy(
      businessMetrics = updatedMetrics.businessMetrics ++ purchaseDates
    )

    updatedMetrics
  }

  // --- Helper methods ---

  /**
   * Merge existing vertices with updates
   */
  private def mergeVertices(vertices: DataFrame, updates: DataFrame): DataFrame = {
    // Use a left anti join to find new vertices
    val newVertices = updates.join(
      vertices,
      updates("id") === vertices("id"),
      "left_anti"
    ).select(updates.columns.map(updates(_)): _*)

    // Union with existing vertices
    vertices.union(newVertices)
  }

  /**
   * Merge existing edges with updates
   */
  private def mergeEdges(edges: DataFrame, updates: DataFrame): DataFrame = {
    // Use a left anti join to find new edges
    val newEdges = updates.join(
      edges,
      (updates("src") === edges("src")) &&
        (updates("dst") === edges("dst")) &&
        (updates("relationship") === edges("relationship")),
      "left_anti"
    ).select(updates.columns.map(updates(_)): _*)

    // Union with existing edges
    edges.union(newEdges)
  }


  /**
   * Create a combined dataset for output
   */
  private def createOutputDataset(vertices: DataFrame, edges: DataFrame): DataFrame = {
    import vertices.sparkSession.implicits._

    // Create a marker column to help split the data later
    val verticesWithMarker = vertices.withColumn("dataset_type", lit("vertex"))
    val edgesWithMarker = edges.withColumn("dataset_type", lit("edge"))

    // Union all the data
    val (verticesWithMarkerAllCols, edgesWithMarkerAllCols) = addMissingColumns(verticesWithMarker, edgesWithMarker)
    val newVerticesDF = verticesWithMarkerAllCols.unionByName(edgesWithMarkerAllCols)
    newVerticesDF
  }

  /**
   * Split the combined output dataset back into components
   */
  private def splitOutputDataset(data: DataFrame): (DataFrame, DataFrame) = {
    val vertices = data.filter(col("dataset_type") === "vertex").drop("dataset_type")
    val edges = data.filter(col("dataset_type") === "edge").drop("dataset_type")

    (vertices, edges)
  }

  private def addMissingColumns(df1: DataFrame, df2: DataFrame): (DataFrame, DataFrame) = {
    val df1Columns = df1.columns.toSet
    val df2Columns = df2.columns.toSet

    // Add missing columns from df2 to df1
    val df1WithMissingCols = df2Columns.diff(df1Columns).foldLeft(df1) {
      (df, colName) => df.withColumn(colName, lit(null))
    }

    // Add missing columns from df1 to df2
    val df2WithMissingCols = df1Columns.diff(df2Columns).foldLeft(df2) {
      (df, colName) => df.withColumn(colName, lit(null))
    }

    (df1WithMissingCols, df2WithMissingCols)
  }

  /**
   * Creates a GraphFrame from vertex and edge DataFrames
   *
   * @param vertices DataFrame with vertex data
   * @param edges    DataFrame with edge data
   * @return GraphFrame representation of the graph
   */
  private def createGraphFrame(vertices: DataFrame, edges: DataFrame): GraphFrame = {
    // GraphFrames expects vertices with 'id' column and edges with 'src' and 'dst' columns
    // which your DataFrames already have
    GraphFrame(vertices, edges)
  }

  /**
   * Runs PageRank algorithm on the graph
   *
   * @param graph      GraphFrame to analyze
   * @param iterations Number of iterations
   * @param resetProb  Reset probability
   * @return DataFrame with vertices and their PageRank scores
   */
  private def runPageRank(graph: GraphFrame, iterations: Int = 10, resetProb: Double = 0.15): DataFrame = {
    val pageRankResults = graph.pageRank
      .resetProbability(resetProb)
      .maxIter(iterations)
      .run()

    // Return vertices with PageRank scores
    pageRankResults.vertices
      .select("id", "vertex_type", "pagerank")
      .orderBy(col("pagerank").desc)
  }

  /**
   * Identifies connected components in the graph
   *
   * @param graph GraphFrame to analyze
   * @return DataFrame with vertices and their component assignments
   */
  private def findConnectedComponents(graph: GraphFrame): DataFrame = {
    // Run connected components algorithm
    val componentResults = graph.connectedComponents.run()

    // Return result with component IDs
    componentResults.select("id", "vertex_type", "component")
      .orderBy("component", "vertex_type", "id")
  }

  /**
   * Finds influential customers based on PageRank
   *
   * @param graph GraphFrame to analyze
   * @param limit Number of top customers to return
   * @return DataFrame with top customers by PageRank
   */
  private def findInfluentialCustomers(graph: GraphFrame, limit: Int = 10): DataFrame = {
    runPageRank(graph)
      .filter(col("vertex_type") === "customer")
      .orderBy(col("pagerank").desc)
      .limit(limit)
  }

  /**
   * Finds popular products based on PageRank
   *
   * @param graph GraphFrame to analyze
   * @param limit Number of top products to return
   * @return DataFrame with top products by PageRank
   */
  private def findPopularProducts(graph: GraphFrame, limit: Int = 10): DataFrame = {
    runPageRank(graph)
      .filter(col("vertex_type") === "product")
      .orderBy(col("pagerank").desc)
      .limit(limit)
  }
}