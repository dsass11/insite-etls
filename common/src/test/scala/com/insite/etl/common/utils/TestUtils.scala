package com.insite.etl.common.utils

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.UUID

/**
 * Utility class for setting up test environments
 */
object TestUtils {

  /**
   * Creates a SparkSession for testing with a unique warehouse directory
   *
   * @return SparkSession configured for testing
   */
  def createTestSparkSession(): SparkSession = {
    // Clean up any existing Derby metastore connections
    shutdownDerby()

    // Generate a unique directory and database name for each test run
    val uniqueId = UUID.randomUUID().toString
    val uniqueDir = s"file:///tmp/hive/warehouse/test-${uniqueId}"

    // Create the session with unique identifiers
    SparkSession.builder()
      .appName("TestSparkSession")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", uniqueDir)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:memory:metastore_${uniqueId};create=true")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Load test data from a resource file
   */
  def loadTestData(
                    spark: SparkSession,
                    resourcePath: String,
                    format: String = "json",
                    options: Map[String, String] = Map("multiline" -> "false")
                  ): DataFrame = {
    // Get the resource URL
    val resource = getClass.getClassLoader.getResource(resourcePath)
    if (resource == null) {
      throw new IllegalArgumentException(s"Resource not found: $resourcePath")
    }

    // Read the data
    spark.read
      .options(options)
      .format(format)
      .load(resource.getPath)
  }
  /**
   * Force Derby to release locks on the metastore database
   */
  def shutdownDerby(): Unit = {
    try {
      // Try to shut down the default database
      try {
        java.sql.DriverManager.getConnection("jdbc:derby:;shutdown=true")
      } catch {
        case _: java.sql.SQLException =>
        // Expected exception - Derby throws exception on successful shutdown
      }

      // Clean up metastore_db directory if it exists
      deleteDirectory(new File("metastore_db"))
    } catch {
      case e: Exception =>
        println(s"Error shutting down Derby: ${e.getMessage}")
    }
  }

  /**
   * Set up a test table with data
   */
  def setupTestTable(
                      spark: SparkSession,
                      df: DataFrame,
                      tableName: String
                    ): Unit = {
    // Create database if necessary
    val dbName = tableName.split("\\.").head
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    // Create the table
    df.write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(tableName)
  }

  /**
   * Clean up test environment
   */
  def cleanupTestEnv(spark: SparkSession): Unit = {
    try {
      // Stop Spark session
      if (spark != null) {
        spark.stop()
      }

      // Shutdown Derby
      shutdownDerby()
    } catch {
      case e: Exception =>
        println(s"Error during cleanup: ${e.getMessage}")
    }
  }
}