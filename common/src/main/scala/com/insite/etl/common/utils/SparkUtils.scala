package com.insite.etl.common.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
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

  def writePartitioned(
                        df: DataFrame,
                        basePath: String,
                        version: String,
                        partitionCols: Array[String]
                      ): Map[String, Array[String]] = {
    // Construct the full path with version
    val fullPath = s"$basePath/$version"

    println(s"Writing partitioned data to $fullPath")
    println(s"Partitioning by columns: ${partitionCols.mkString(", ")}")

    // Write the data with partitioning
    df.write
      .partitionBy(partitionCols: _*)
      .format("parquet")
      .mode("overwrite")
      .save(fullPath)

    // Get distinct values for each partition column, always converting to string
    val partitionValues = partitionCols.map { colName =>
      // Cast to string in SQL to avoid type issues
      val values = df.select(col(colName).cast("string").as(colName))
        .distinct()
        .collect()
        .map(_.getString(0))

      colName -> values
    }.toMap

    println(s"Wrote data with partition values: $partitionValues")
    partitionValues
  }

  /**
   * Update table metadata to point to the new partition location
   *
   * @param tableName Table to update
   * @param partitionCols Partition columns
   * @param partitionValues Values for each partition column
   * @param basePath Base data path
   * @param version Version/timestamp directory
   */
  def updatePartitionedTableMetadata(
                                      tableName: String,
                                      partitionCols: Array[String],
                                      partitionValues: Map[String, Array[String]],
                                      basePath: String,
                                      version: String
                                    )(implicit spark: SparkSession): Unit = {
    // Make sure the table exists
    val tableExists = spark.catalog.tableExists(tableName)
    if (!tableExists) {
      throw new IllegalStateException(s"Table $tableName does not exist. Create it before updating metadata.")
    }

    val fullPath = s"$basePath/$version"

    // First set the table location to the latest version directory
    spark.sql(s"ALTER TABLE $tableName SET LOCATION '$fullPath'")
    println(s"Updated table $tableName location to point to version: $version")

    // Then refresh all partition metadata
    generatePartitionSpecs(partitionCols, partitionValues).foreach { partitionSpec =>
      val partitionPath = partitionSpec.map { case (col, value) => s"$col=$value" }.mkString("/")
      val fullPartitionPath = s"$fullPath/$partitionPath"

      val partitionClause = partitionSpec.map { case (col, value) =>
        s"$col='$value'"
      }.mkString(", ")

      val sql = s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION ($partitionClause) LOCATION '$fullPartitionPath'"
      println(s"Executing: $sql")
      spark.sql(sql)
    }

    println(s"All partitions registered for table $tableName")
  }

  /**
   * Generate all partition specifications from the partition columns and values
   *
   * @param partitionCols Partition columns
   * @param partitionValues Values for each partition column
   * @return List of all partition specifications
   */
  private def generatePartitionSpecs(
                                      partitionCols: Array[String],
                                      partitionValues: Map[String, Array[String]]
                                    ): List[Map[String, String]] = {

    def generatePartitionSpecsHelper(
                                      remainingCols: List[String],
                                      currentSpec: Map[String, String]
                                    ): List[Map[String, String]] = {
      if (remainingCols.isEmpty) {
        List(currentSpec)
      } else {
        val col = remainingCols.head
        val values = partitionValues.getOrElse(col, Array.empty[String])

        values.flatMap { value =>
          generatePartitionSpecsHelper(
            remainingCols.tail,
            currentSpec + (col -> value)
          )
        }.toList
      }
    }

    generatePartitionSpecsHelper(partitionCols.toList, Map.empty)
  }
}