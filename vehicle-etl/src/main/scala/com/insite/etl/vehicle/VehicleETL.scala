package com.insite.etl.vehicle

import com.insite.etl.common.ETL
import com.insite.etl.common.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Implementation of ETL for vehicle data processing
 */
class VehicleETL extends ETL {

  /**
   * Process the events data
   *
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   * @return DataFrame with processed data
   */
  override def doLogic()(implicit spark: SparkSession, properties: Map[String, String]): DataFrame = {
    // Get source table from properties, with default fallback
    val sourceTable = properties.getOrElse("source-table", "vehicle_datalake.events")
    println(s"Reading from source table: $sourceTable")

    // Read from the configured source table
    val eventsDF = spark.sql(s"SELECT * FROM $sourceTable")

    // Apply transformations
    val processedDF = eventsDF
      .withColumn("manufacturer", trim(col("manufacturer")))
      .filter(col("vin").isNotNull)
      .withColumn("gearPosition",
        when(col("gearPosition") === "D", "4")
          .when(col("gearPosition") === "R", "-1")
          .when(col("gearPosition") === "N", "0")
          .when(col("gearPosition") === "P", "0")
          .otherwise(col("gearPosition")))
      // Add partition columns based on timestamp
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))

    // Return the processed data
    processedDF
  }

  /**
   * Save the processed data with partitioning support
   *
   * @param data       DataFrame to save
   * @param spark      Implicit SparkSession
   * @param properties Implicit configuration properties
   */
  override def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit = {
    // Get configuration properties with defaults
    val targetTable = properties.getOrElse("target-table", "vehicle_datalake.processed_events")
    val basePath = properties.getOrElse("base-path", "s3://my-bucket/data")
    val timestamp = System.currentTimeMillis().toString
    val version = properties.getOrElse("version", s"v$timestamp")

    // Extract partition columns
    val partitionCols = properties.get("partition-columns").map(_.split(",").map(_.trim))

    // Handle based on whether partition columns are specified
    partitionCols match {
      case None | Some(Array()) =>
        println("ERROR: No partition columns specified. Please set partition-columns property.")
        throw new IllegalArgumentException("Partition columns must be specified")

      case Some(columns) =>
        println(s"Writing partitioned data with columns: ${columns.mkString(", ")}")

        // Write data with partitioning
        val partitionValues = SparkUtils.writePartitioned(data, basePath, version, columns)

        // Update table metadata to point to the new partition location
        SparkUtils.updatePartitionedTableMetadata(targetTable, columns, partitionValues, basePath, version)

        println(s"Successfully updated partitioned table $targetTable")
    }
  }
}