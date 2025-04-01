package com.insite.etl.common.metrics

// Core metrics model
case class ETLMetrics(
                       jobId: String,
                       etlClass: String,
                       name: String, // Added metric name field
                       startTime: Long,
                       endTime: Option[Long] = None,
                       status: String = "RUNNING", // RUNNING, SUCCESS, FAILED
                       recordCounts: Map[String, Long] = Map.empty,
                       timings: Map[String, Long] = Map.empty,
                       errorDetails: Option[String] = None,
                       businessMetrics: Map[String, Any] = Map.empty
                     ) {
  def duration: Option[Long] = endTime.map(_ - startTime)

  def withEndTime(end: Long): ETLMetrics = copy(endTime = Some(end))

  def withStatus(newStatus: String): ETLMetrics = copy(status = newStatus)

  def withError(error: String): ETLMetrics = copy(status = "FAILED", errorDetails = Some(error))

  def addRecordCount(phase: String, count: Long): ETLMetrics =
    copy(recordCounts = recordCounts + (phase -> count))

  def addTiming(phase: String, time: Long): ETLMetrics =
    copy(timings = timings + (phase -> time))

  def addBusinessMetric(key: String, value: Any): ETLMetrics =
    copy(businessMetrics = businessMetrics + (key -> value))
}