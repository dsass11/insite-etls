package com.insite.etl.common.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.encoders.RowEncoder


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
   * Write data with partitioning
   *
   * @param df            DataFrame to write
   * @param basePath      Base path for the data
   * @param version       Version/timestamp directory
   * @param partitionCols Columns to partition by
   * @return Map of partition columns and their values
   */

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

    // Create accumulators for each partition column
    val spark = df.sparkSession
    val accumulators = partitionCols.map { colName =>
      colName -> spark.sparkContext.collectionAccumulator[String](s"partition_values_$colName")
    }.toMap

    // Create a copy of the DataFrame with partition value collection
    // Use RowEncoder to provide the implicit encoder for Row
    val rowEncoder = RowEncoder(df.schema)
    val dfWithAccumulators = df.mapPartitions { partition =>
      partition.map { row =>
        // For each partition column, collect its value in the accumulator
        partitionCols.foreach { colName =>
          val fieldIndex = row.fieldIndex(colName)
          val fieldValue = row.get(fieldIndex)
          if (fieldValue != null) {
            accumulators(colName).add(fieldValue.toString)
          }
        }
        // Return the row unchanged
        row
      }
    }(rowEncoder)

    // Write the data with partitioning
    dfWithAccumulators.write
      .partitionBy(partitionCols: _*)
      .format("parquet")
      .mode("overwrite")
      .save(fullPath)

    // Get partition values from accumulators
    val accumulatedPartitionValues = partitionCols.map { colName =>
      val accumulator = accumulators(colName)
      val values = accumulator.value.asScala.toSet.toArray
      colName -> values
    }.toMap

    println(s"Wrote data with partition values: $accumulatedPartitionValues")
    accumulatedPartitionValues
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