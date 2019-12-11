//package com.datalogs.sparkmetrics
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import com.datalogs.metrics.profiler.util.BasClient
//import com.datalogs.Logging
//
//case class MetricsPublishFailure(error: Throwable, orig: Any)
//case class MetricsTick(tickType:String = "COUNT", elapsedTime:Int = 10, tickTotal:Long = 1, tickMin:Long = 1, tickMax:Long =1, tickCount:Long = 1)
//case class MetricsSrvMetaInfo(taskName:String = "ds", serviceId:Int = 218481, hostId:Int = 13686)
//
//case class MetricsPublisher(publishMetrics : Boolean, dataSource : String) extends Logging {
//
//  private final val clientServiceName = "client-service"
//  val hostName:String = java.net.InetAddress.getLocalHost().getHostName()
//  private final val overrideBasHost = hostName.toLowerCase.charAt(0) match {
//    case  'd' => "dev-dns.datalogs.com"
//    case  'p' => "prod-dns.datalogs.com"
//    case  _ => "localhost"
//  }
//  private val hostPrefix:String = hostName.substring(0,4)
//  private final val metricPublisher = new BasClient(overrideBasHost, clientServiceName, 1, 5)
//  private final val expectedTickTypes = Array("UNSPECIFIED", "TOTAL", "COUNT", "MIN","MAX", "AVG", "RATE")
//
//  private def buildRequest(metricName:String, categoryName:String, srvMetaInfo: SrvMetaInfo, tickDetails: Tick):String={
//    val timestamp: String = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date)
//    val sb = new StringBuilder("<request><publishMetricsRequest>")
//
//    sb.append("<machineNumber>").append(srvMetaInfo.hostId).append("</machineNumber>")
//    sb.append("<taskName>").append(srvMetaInfo.taskName).append("</taskName>")
//    sb.append("<timestamp>").append(timestamp).append("</timestamp>")
//    sb.append("<groups>")
//    sb.append("<elapsedTime>").append(tickDetails.elapsedTime).append("</elapsedTime>")
//    sb.append("<records>")
//    sb.append("<category>").append(srvMetaInfo.serviceId).append("-").append("APP-").append(categoryName).append(dataSource.toUpperCase).append("</category>")
//    sb.append("<name>").append(hostPrefix.toUpperCase).append(metricName).append("</name>")
//    sb.append("<count>").append(tickDetails.tickCount).append("</count>")
//    sb.append("<total>").append(tickDetails.tickTotal).append("</total>")
//    sb.append("<min>").append(tickDetails.tickMin).append("</min>")
//    sb.append("<max>").append(tickDetails.tickMax).append("</max>")
//    sb.append("<publicationType>").append(tickDetails.tickType).append("</publicationType>")
//    sb.append("</records>")
//    sb.append("</groups>")
//    sb.append("</publishMetricsRequest></request>").toString()
//  }
//
//  /**
//    * Overloaded publish Metric method by category and metric name
//    *
//    * @param categoryName
//    * @param metricName
//    * @param tickDetails
//    * @return
//    */
//  final def publishMetric(categoryName:String, metricName:String, tickDetails: Tick = Tick()) = {
//    try {
//      if (!(expectedTickTypes contains tickDetails.tickType)) throw new IllegalArgumentException("Invalid Tick Type")
//
//      //Use default service host meta Info if not available
//      val svrMetaInfo = MetricsSrvMetaInfo()
//
//      val request= buildRequest(metricName, categoryName, svrMetaInfo, tickDetails)
//      logInfo("Listener : Running on host " + hostName + " publishing to farm " + overrideBasHost)
//      logInfo("Listener : " + request)
//      if (publishMetrics) {
//        metricPublisher.send(request)
//        logInfo("Listener : Successfully published Metrics")
//      }
//    }  catch {
//      case e:Exception =>
//        logError(s"Listener Unable to publish Metric ${e}" )
//    }
//  }
//}