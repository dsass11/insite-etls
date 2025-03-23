package com.insite.etl.vehicle

import com.insite.etl.common.ETL
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

    // Return the processed data
    processedDF
  }

  /**
   * Save the processed data
   *
   * @param data DataFrame to save
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   */
  override def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit = {
    // Get target table from properties, with default fallback
    val targetTable = properties.getOrElse("target-table", "vehicle_datalake.processed_events")
    println(s"Writing to target table: $targetTable")

    // Create a table for the processed data
    data.write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(targetTable)

    println(s"Saved ${data.count()} records to $targetTable table")
  }
}