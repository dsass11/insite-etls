package com.insite.etl.common

import org.apache.spark.sql.{DataFrame, SparkSession}

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
}