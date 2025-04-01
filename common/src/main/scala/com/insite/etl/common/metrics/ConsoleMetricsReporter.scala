package com.insite.etl.common.metrics

trait MetricsReporter {
  def report(metrics: ETLMetrics): Unit
}

// Simple console reporter for development/testing
class ConsoleMetricsReporter extends MetricsReporter {
  override def report(metrics: ETLMetrics): Unit = {
    println("===== ETL METRICS REPORT =====")
    println(s"Job ID: ${metrics.jobId}")
    println(s"ETL Class: ${metrics.etlClass}")
    println(s"Metric Name: ${metrics.name}")
    println(s"Status: ${metrics.status}")
    println(s"Start Time: ${new java.util.Date(metrics.startTime)}")
    metrics.endTime.foreach(end => println(s"End Time: ${new java.util.Date(end)}"))
    metrics.duration.foreach(dur => println(s"Duration: ${dur}ms"))

    println("\nRecord Counts:")
    metrics.recordCounts.foreach { case (phase, count) =>
      println(s"  - $phase: $count records")
    }

    println("\nTimings:")
    metrics.timings.foreach { case (phase, time) =>
      println(s"  - $phase: ${time}ms")
    }

    println("\nBusiness Metrics:")
    metrics.businessMetrics.foreach { case (key, value) =>
      println(s"  - $key: $value")
    }

    metrics.errorDetails.foreach { error =>
      println("\nError Details:")
      println(error)
    }
    println("===============================")
  }
}