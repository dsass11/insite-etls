package com.insite.etl.common

import com.insite.etl.common.utils.SparkUtils

/**
 * Generic ETL runner that can execute any ETL implementation
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

    println(s"Running ETL: $etlClassName")
    println(s"Configuration: $properties")

    // Create Spark session
    implicit val spark = SparkUtils.createSparkSession()

    try {
      // Dynamically load and instantiate the ETL class
      val etlClass = Class.forName(etlClassName)
      val etl = etlClass.getDeclaredConstructor().newInstance().asInstanceOf[ETL]

      // Run the ETL with implicit configuration
      val processedData = etl.doLogic()
      etl.doOutput(processedData)

      println(s"Successfully executed ETL: $etlClassName")
    } catch {
      case e: Exception =>
        println(s"Error executing ETL $etlClassName: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
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
}