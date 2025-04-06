package com.insite.etl.vehicle

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.insite.etl.common.metrics.ETLMetrics
import org.apache.spark.sql.functions.{to_date, from_unixtime, hour}
import org.scalatest.BeforeAndAfterAll

class VehicleMetricsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  // Create a test Spark session
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("VehicleMetricsTest")
    .getOrCreate()

  import spark.implicits._

  test("VehicleETL should collect business metrics correctly") {
    implicit val properties: Map[String, String] = Map.empty

    // Create test data
    val testData = Seq(
      (1, "Toyota", "Camry", 2020, "Blue", "VIN1", "Toyota", "D", 1617235200000L),
      (2, "Honda", "Accord", 2021, "Black", "VIN2", "Honda", "P", 1617321600000L),
      (3, "Ford", "F-150", 2019, "Red", "VIN3", "Ford", "N", 1617408000000L),
      (4, "Toyota", "Corolla", 2021, "Silver", "VIN4", "Toyota", "D", 1617494400000L)
    ).toDF("id", "make", "model", "year", "color", "vin", "manufacturer", "gearPosition", "timestamp")
      .withColumn("date", to_date(from_unixtime($"timestamp" / 1000)))
      .withColumn("hour", hour(from_unixtime($"timestamp" / 1000)))

    val vehicleETL = new VehicleETL()

    val metrics = ETLMetrics(
      jobId = "test-vehicle-job",
      etlClass = "VehicleETL",
      name = "Vehicle Metrics Test",
      startTime = System.currentTimeMillis()
    )

    val updatedMetrics = vehicleETL.collectBusinessMetrics(testData, metrics)(spark, properties)

    // Test manufacturer counts
    updatedMetrics.businessMetrics should contain key "count_Toyota"
    updatedMetrics.businessMetrics should contain key "count_Honda"
    updatedMetrics.businessMetrics should contain key "count_Ford"

    updatedMetrics.businessMetrics("count_Toyota") should be (2)
    updatedMetrics.businessMetrics("count_Honda") should be (1)
    updatedMetrics.businessMetrics("count_Ford") should be (1)

    // Test gear position counts
    updatedMetrics.businessMetrics should contain key "gear_pos_D"
    updatedMetrics.businessMetrics should contain key "gear_pos_P"
    updatedMetrics.businessMetrics should contain key "gear_pos_N"

    updatedMetrics.businessMetrics("gear_pos_D") should be (2)
    updatedMetrics.businessMetrics("gear_pos_P") should be (1)
    updatedMetrics.businessMetrics("gear_pos_N") should be (1)

    // Test date range metrics
    updatedMetrics.businessMetrics should contain key "min_date"
    updatedMetrics.businessMetrics should contain key "max_date"
  }

  // Clean up after tests
  override def afterAll(): Unit = {
    spark.stop()
  }
}