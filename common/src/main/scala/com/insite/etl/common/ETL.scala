package com.insite.etl.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.insite.etl.common.metrics.ETLMetrics

/**
 * Common trait for ETL operations
 */
trait ETL {
  /**
   * Process the data and apply transformations
   *
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   * @return DataFrame with the processed data
   */
  def doLogic()(implicit spark: SparkSession, properties: Map[String, String]): DataFrame

  /**
   * Output the processed data to the destination
   *
   * @param data DataFrame to output
   * @param spark Implicit SparkSession
   * @param properties Implicit configuration properties
   */
  def doOutput(data: DataFrame)(implicit spark: SparkSession, properties: Map[String, String]): Unit

  /**
   * Collect business-specific metrics from the processed data
   * This should be overridden by implementations to provide custom metrics
   *
   * @param data       DataFrame to analyze
   * @param metrics    Current metrics object to enhance
   * @param spark      Implicit SparkSession
   * @param properties Implicit configuration properties
   * @return Updated metrics object with business metrics
   */
  def collectBusinessMetrics(data: DataFrame, metrics: ETLMetrics)
                            (implicit spark: SparkSession, properties: Map[String, String]): ETLMetrics = {
    // Default implementation just returns the metrics unchanged
    metrics
  }
}