//package com.datalogs.sparkmetrics
//
//import java.io.Serializable
//
//import com.datalogs.Logging
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.SparkContext
//import org.joda.time.DateTime
//
//import scala.util.control.NonFatal
//
//
//case class SparkJobMetricsPublisherContext(metricPublisher : MetricsPublisher, datasourceName : String)
//
//trait SparkJobMetricsPublisher {
//  val publisherContext : SparkJobMetricsPublisherContext
//  def publish(batchId : String) : Unit
//  def reset : Unit
//}
//
//class SparkJobTimePublisher(val publisherContext : SparkJobMetricsPublisherContext) extends SparkJobMetricsPublisher with Logging {
//
//  override def publish(batchId : String) : Unit = {
//    // runTime is in seconds ...
//    val runTime = SparkJobMetrics.getJobRunTimeInSeconds
//    logInfo("Listener Metrics --> Total Job Run Time for batch " + batchId + " is " + runTime)
//
//    if (publisherContext.metricPublisher.publishMetrics && runTime > 0)
//      publisherContext.metricPublisher.publishMetric(categoryName = "SPARK", metricName = "RunTime", tickDetails = Tick(tickType = "COUNT", tickTotal = runTime, tickMin = runTime, tickMax = runTime, tickCount = runTime))
//
//    reset
//  }
//
//  override def reset: Unit = SparkJobMetrics.resetJobTime
//}
//
//class SparkJobCountersPublisher(val publisherContext : SparkJobMetricsPublisherContext, sc : SparkContext, trapDir : String) extends SparkJobMetricsPublisher with Logging {
//  private[this] val DEFRECS = 0L
//
//  private def countTrapRecords(): Long = {
//    try {
//      logInfo("Listener:->" + trapDir)
//      val fs = FileSystem.get(sc.hadoopConfiguration)
//      if (fs.exists(new Path(trapDir))) sc.textFile(trapDir.toString).count else 0
//    }
//    catch {
//      case NonFatal(e) => {
//        logWarning("Failed to count bad records, reporting 0")
//        0
//      }
//    }
//  }
//
//  override def publish(batchId : String) : Unit = {
//    val recsWritten = SparkJobMetrics.totRecordsWritten
//    val recsRead = SparkJobMetrics.totRecordsRead
//    val badRecs = countTrapRecords()
//
//    logInfo("Listener Metrics --> Total number of records read for batch " + batchId + " is " + recsRead)
//    logInfo("Listener Metrics --> Total number of records written for batch " + batchId + " is " + recsWritten)
//    logInfo("Listener Metrics --> Total number of error records for batch " + batchId + " is " + badRecs)
//
//    publisherContext.metricPublisher.publishMetrics match {
//      case true => {
//        if (recsRead > 0) publisherContext.metricPublisher.publishMetric(categoryName = "SPARK", metricName = "RecordsProcessed", tickDetails = Tick(tickType = "COUNT", tickTotal = recsRead, tickMin = recsRead, tickMax = recsRead, tickCount = recsRead))
//        if (recsWritten > 0) publisherContext.metricPublisher.publishMetric(categoryName = "SPARK", metricName = "RecordsWritten", tickDetails = Tick(tickType = "COUNT", tickTotal = recsWritten, tickMin = recsWritten, tickMax = recsWritten, tickCount = recsWritten))
//        if (badRecs > 0) publisherContext.metricPublisher.publishMetric(categoryName = "SPARK", metricName = "ErrorRecords", tickDetails = Tick(tickType = "COUNT", tickTotal = badRecs, tickMin = badRecs, tickMax = badRecs, tickCount = badRecs))
//      }
//
//      case false => //Do Nothing
//    }
//
//    reset
//  }
//
//  override def reset: Unit = SparkJobMetrics.resetJobCounters
//}
//
//
