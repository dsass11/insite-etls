package com.insite.etl.common.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utility functions for working with Spark
 */
object SparkUtils {

  /**
   * Creates a SparkSession with Hive support
   *
   * @param appName The name of the application
   * @param warehouseDir Directory for the Hive warehouse
   * @param enableHive Whether to enable Hive support
   * @return A configured SparkSession
   */
  def createSparkSession(
                          appName: String = "InsiteETL",
                          warehouseDir: String = "file:///tmp/hive/warehouse",
                          enableHive: Boolean = true
                        ): SparkSession = {
    val sessionBuilder = SparkSession.builder()
      .appName(appName)
      .master("local[*]") // Use local mode for development
      .config("spark.sql.warehouse.dir", warehouseDir)

    val finalBuilder = if (enableHive) {
      sessionBuilder.enableHiveSupport()
    } else {
      sessionBuilder
    }

    finalBuilder.getOrCreate()
  }

  /**
   * Execute a SQL query and return the result
   *
   * @param spark SparkSession
   * @param query SQL query to execute
   * @return DataFrame with the query result
   */
  def executeSql(spark: SparkSession, query: String): DataFrame = {
    spark.sql(query)
  }

  /**
   * Create database if it doesn't exist
   *
   * @param spark SparkSession
   * @param dbName Name of the database
   */
  def createDatabase(spark: SparkSession, dbName: String): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    println(s"Database created (if not exists): $dbName")
  }

  /**
   * Create a table from a DataFrame
   *
   * @param spark SparkSession
   * @param df DataFrame to save as a table
   * @param tableName Full table name (database.table)
   * @param partitionCols Optional partition columns
   * @param mode Save mode (overwrite by default)
   */
  def createTable(
                   spark: SparkSession,
                   df: DataFrame,
                   tableName: String,
                   partitionCols: Seq[String] = Seq.empty,
                   mode: String = "overwrite"
                 ): Unit = {
    // Drop table if exists
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create writer
    val writer = df.write
      .format("parquet")
      .mode(mode)

    // Add partitioning if specified
    val finalWriter = if (partitionCols.nonEmpty) {
      writer.partitionBy(partitionCols: _*)
    } else {
      writer
    }

    // Save as table
    finalWriter.saveAsTable(tableName)

    println(s"Table created: $tableName")
  }

  /**
   * Show all tables in a database
   *
   * @param spark SparkSession
   * @param dbName Database name
   */
  def showTables(spark: SparkSession, dbName: String): Unit = {
    val tables = spark.sql(s"SHOW TABLES IN $dbName")
    println(s"Tables in $dbName:")
    tables.show()
  }

  /**
   * Describe a table structure
   *
   * @param spark SparkSession
   * @param tableName Full table name (database.table)
   */
  def describeTable(spark: SparkSession, tableName: String): Unit = {
    val description = spark.sql(s"DESCRIBE TABLE $tableName")
    println(s"Structure of $tableName:")
    description.show(100, false)
  }
}