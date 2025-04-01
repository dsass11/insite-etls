package com.insite.etl.common

import com.insite.etl.common.utils.SparkUtils
import com.insite.etl.common.metrics.{ETLMetrics, MetricsReporter, ConsoleMetricsReporter}
import org.apache.spark.sql.DataFrame

import java.util.UUID

/**
 * Enhanced ETL runner that can execute any ETL implementation with metrics
 */
object ETLRunner {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: ETLRunner <etl-class-name> [param1=value1 param2=value2 ...]")
      System.exit(1)
    }

    val etlClassName = args(0)

    // Parse configuration parameters from arguments
    implicit val properties: Map[String, String] = parseConfig(args.drop(1))

    // Create a unique job ID
    val jobId = properties.getOrElse("job-id", UUID.randomUUID().toString)

    println(s"Running ETL: $etlClassName (Job ID: $jobId)")
    println(s"Configuration: $properties")

    // Initialize metrics
    var metrics = ETLMetrics(
      jobId = jobId,
      etlClass = etlClassName,
      name = properties.getOrElse("metrics-name", "ETL Run"),
      startTime = System.currentTimeMillis()
    )

    // Create metrics reporter
    val reporter = createReporter(properties)

    // Create Spark session
    implicit val spark = SparkUtils.createSparkSession()

    try {
      // Dynamically load and instantiate the ETL class
      val etlClass = Class.forName(etlClassName)
      val etl = etlClass.getDeclaredConstructor().newInstance().asInstanceOf[ETL]

      // Time and execute the logic phase
      val logicStart = System.currentTimeMillis()
      val processedData = etl.doLogic()
      val logicEnd = System.currentTimeMillis()

      // Update metrics with logic phase timing
      metrics = metrics.addTiming("doLogic", logicEnd - logicStart)

      // Add record count after logic phase
      val recordCount = processedData.count()
      metrics = metrics.addRecordCount("processed", recordCount)

      // Collect business metrics
      metrics = etl.collectBusinessMetrics(processedData, metrics)

      // Time and execute the output phase
      val outputStart = System.currentTimeMillis()
      etl.doOutput(processedData)
      val outputEnd = System.currentTimeMillis()

      // Update metrics with output phase timing
      metrics = metrics.addTiming("doOutput", outputEnd - outputStart)

      // Mark as successful
      metrics = metrics.withStatus("SUCCESS").withEndTime(System.currentTimeMillis())

      println(s"Successfully executed ETL: $etlClassName")
    } catch {
      case e: Exception =>
        // Update metrics with error information
        metrics = metrics.withStatus("FAILED")
          .withError(s"${e.getClass.getName}: ${e.getMessage}")
          .withEndTime(System.currentTimeMillis())

        println(s"Error executing ETL $etlClassName: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      // Report metrics
      reporter.report(metrics)

      // Stop Spark session
      spark.stop()
    }
  }

  /**
   * Parse command line arguments into a configuration map
   * Accepts parameters in the format: key=value
   *
   * @param args Command line arguments
   * @return Map of configuration parameters
   */
  private def parseConfig(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      val parts = arg.split("=", 2)
      if (parts.length == 2) {
        Some(parts(0) -> parts(1))
      } else {
        println(s"Warning: Ignoring malformed parameter: $arg")
        None
      }
    }.toMap
  }

  /**
   * Create a metrics reporter based on configuration
   *
   * @param properties Configuration properties
   * @return MetricsReporter implementation
   */
  private def createReporter(properties: Map[String, String]): MetricsReporter = {
    val reporterType = properties.getOrElse("metrics-reporter", "console")

    reporterType match {
      case "console" => new ConsoleMetricsReporter()
      // Add additional reporters as needed
      case _ =>
        println(s"Unknown metrics reporter type: $reporterType, defaulting to console")
        new ConsoleMetricsReporter()
    }
  }
}