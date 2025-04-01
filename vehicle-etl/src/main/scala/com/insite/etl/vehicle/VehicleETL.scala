package com.insite.etl.vehicle

import com.insite.etl.common.ETL
import com.insite.etl.common.utils.SparkUtils
import com.insite.etl.common.metrics.ETLMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Implementation of ETL for vehicle data processing with metrics support
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
    // Implementation remains the same as before
    val sourceTable = properties.getOrElse("source-table", "vehicle_datalake.events")
    println(s"Reading from source table: $sourceTable")

    val eventsDF = spark.sql(s"SELECT * FROM $sourceTable")

    val processedDF = eventsDF
      .withColumn("manufacturer", trim(col("manufacturer")))
      .filter(col("vin").isNotNull)
      .withColumn("gearPosition",
        when(col("gearPosition") === "D", "4")
          .when(col("gearPosition") === "R", "-1")
          .when(col("gearPosition") === "N", "0")
          .when(col("gearPosition") === "P", "0")
          .otherwise(col("gearPosition")))
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))

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
    // Implementation remains the same as before - no changes needed
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

  /**
   * Collect business-specific metrics for vehicle data
   *
   * @param data DataFrame to analyze
   * @param metrics Current metrics object to enhance
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   * @return Updated metrics object with business metrics
   */
  override def collectBusinessMetrics(data: DataFrame, metrics: ETLMetrics)
                                     (implicit spark: SparkSession, properties: Map[String, String]): ETLMetrics = {
    import spark.implicits._

    // Start with the current metrics
    var updatedMetrics = metrics

    // Add data quality metrics - count nulls in key columns
    val keyColumns = Seq("vin", "manufacturer", "model", "year")
    keyColumns.foreach { colName =>
      val nullCount = data.filter(col(colName).isNull).count()
      val totalCount = data.count()
      val nullPercentage = if (totalCount > 0) (nullCount.toDouble / totalCount * 100) else 0.0
      updatedMetrics = updatedMetrics.addBusinessMetric(s"null_pct_$colName", nullPercentage)
    }

    // Count vehicles by manufacturer
    val manufacturerCounts = data
      .groupBy("manufacturer")
      .count()
      .collect()

    manufacturerCounts.foreach { row =>
      val manufacturer = row.getAs[String]("manufacturer")
      val count = row.getAs[Long]("count")
      updatedMetrics = updatedMetrics.addBusinessMetric(s"count_$manufacturer", count)
    }

    // Count by gear position
    val gearCounts = data
      .groupBy("gearPosition")
      .count()
      .collect()

    gearCounts.foreach { row =>
      val gearPosition = row.getAs[String]("gearPosition")
      val count = row.getAs[Long]("count")
      updatedMetrics = updatedMetrics.addBusinessMetric(s"gear_pos_$gearPosition", count)
    }

    // Date range metrics
    val dateStats = data.agg(
      min("date").as("min_date"),
      max("date").as("max_date")
    ).collect().head

    updatedMetrics = updatedMetrics
      .addBusinessMetric("min_date", dateStats.getAs[String]("min_date"))
      .addBusinessMetric("max_date", dateStats.getAs[String]("max_date"))

    // Return the updated metrics
    updatedMetrics
  }
}