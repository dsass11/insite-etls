package com.insite.etl.common.utils

import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import java.io.File
import java.util.UUID

/**
 * Utility class for setting up test environments
 */
object TestUtils {

  /**
   * Creates a SparkSession for testing with a unique warehouse directory
   *
   * @param appName Optional name for the test application
   * @param enableHive Whether to enable Hive support
   * @return SparkSession configured for testing
   */
  def createTestSparkSession(
                              appName: String = "TestSparkSession",
                              enableHive: Boolean = true
                            ): SparkSession = {
    // Clean up any existing Derby metastore connections
    shutdownDerby()

    // Generate a unique directory and database name for each test run
    val uniqueId = UUID.randomUUID().toString
    val uniqueDir = s"file:///tmp/hive/warehouse/test-${uniqueId}"

    // Create the session with unique identifiers and Spark 3 configuration
    val builder = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", uniqueDir)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:memory:metastore_${uniqueId};create=true")
      // Spark 3 specific configurations
      // In SparkUtils.scala and TestUtils.scala
      .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
      .config("spark.sql.adaptive.enabled", "true")
      // Enable non-empty location for managed tables (Spark 3 feature)
      .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

    val finalBuilder = if (enableHive) {
      builder.enableHiveSupport()
    } else {
      builder
    }

    finalBuilder.getOrCreate()
  }

  /**
   * Load test data from a resource file
   *
   * @param spark SparkSession
   * @param resourcePath Path to the resource file
   * @param format Format of the data (json, csv, parquet, etc.)
   * @param options Options for reading the data
   * @return DataFrame with the loaded data
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
   * Create a test DataFrame with sample data
   *
   * @param spark SparkSession
   * @param data Sequence of data rows
   * @param schema DataFrame schema (column names)
   * @return DataFrame with the sample data
   */
  def createTestDataFrame(
                           spark: SparkSession,
                           data: Seq[Seq[Any]],
                           schema: Seq[String]
                         ): DataFrame = {
    // Convert to Row objects
    import org.apache.spark.sql.Row
    val rows = data.map(Row.fromSeq(_))

    // Create schema
    import org.apache.spark.sql.types._
    val schemaStruct = StructType(
      schema.zipWithIndex.map { case (fieldName, i) =>
        val dataType = if (rows.isEmpty || rows.head.get(i) == null) {
          StringType
        } else {
          rows.head.get(i) match {
            case _: String => StringType
            case _: Int | _: java.lang.Integer => IntegerType
            case _: Long | _: java.lang.Long => LongType
            case _: Double | _: java.lang.Double => DoubleType
            case _: Boolean | _: java.lang.Boolean => BooleanType
            case _ => StringType
          }
        }
        StructField(fieldName, dataType, nullable = true)
      }
    )

    // Create DataFrame
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schemaStruct
    )
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
   *
   * @param spark SparkSession
   * @param df DataFrame with the data
   * @param tableName Name of the table to create
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
   * Create an empty table with the same schema as a DataFrame
   *
   * @param spark SparkSession
   * @param schema DataFrame to use as template for schema
   * @param tableName Name of the empty table to create
   */
  def createEmptyTable(
                        spark: SparkSession,
                        schema: DataFrame,
                        tableName: String
                      ): Unit = {
    val dbName = tableName.split("\\.").head
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    // Create empty DataFrame with same schema
    import spark.implicits._
    val emptyDf = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      schema.schema
    )

    // Save as table
    emptyDf.write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(tableName)
  }

  /**
   * Clean up test environment
   *
   * @param spark SparkSession to stop
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

  /**
   * Compare two DataFrames and return whether they are equal
   *
   * @param df1 First DataFrame
   * @param df2 Second DataFrame
   * @param checkNullability Whether to check nullability of columns
   * @return True if the DataFrames are equal, false otherwise
   */
  def dataFramesEqual(
                       df1: DataFrame,
                       df2: DataFrame,
                       checkNullability: Boolean = false
                     ): Boolean = {
    // Check schema equality
    val schema1 = df1.schema
    val schema2 = df2.schema
    val schemaEqual = if (checkNullability) {
      schema1.equals(schema2)
    } else {
      // Check schema equality ignoring nullability
      schema1.fields.length == schema2.fields.length &&
        schema1.fields.zip(schema2.fields).forall { case (f1, f2) =>
          f1.name.equals(f2.name) && f1.dataType.equals(f2.dataType)
        }
    }

    if (!schemaEqual) {
      println("Schemas do not match!")
      println(s"Schema 1: ${schema1.treeString}")
      println(s"Schema 2: ${schema2.treeString}")
      return false
    }

    // Check data equality
    val count1 = df1.count()
    val count2 = df2.count()

    if (count1 != count2) {
      println(s"Row counts do not match: $count1 vs $count2")
      return false
    }

    // For empty DataFrames, we've already checked schema, so return true
    if (count1 == 0) {
      return true
    }

    // Compare data by collecting and sorting
    import org.apache.spark.sql.functions._

    // Sort both DataFrames by all columns to ensure consistent ordering
    val sortCols = df1.columns.map(col)
    val df1Sorted = df1.sort(sortCols: _*)
    val df2Sorted = df2.sort(sortCols: _*)

    // Collect and compare
    val rows1 = df1Sorted.collect()
    val rows2 = df2Sorted.collect()

    val rowsEqual = rows1.zip(rows2).forall { case (r1, r2) =>
      r1.equals(r2)
    }

    if (!rowsEqual) {
      println("Data does not match!")
      println("First few rows of DataFrame 1:")
      df1Sorted.show(5)
      println("First few rows of DataFrame 2:")
      df2Sorted.show(5)
    }

    rowsEqual
  }
}