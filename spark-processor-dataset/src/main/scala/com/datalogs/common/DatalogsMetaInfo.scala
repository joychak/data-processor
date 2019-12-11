package com.datalogs.common

import com.datalogs.dataschema.MetaInfo
import org.joda.time.{DateTime, DateTimeZone}

object DatalogsMetaInfo extends ((Long, String, String, String, String) => MetaInfo) {
  val processingUTC = new DateTime(DateTimeZone.UTC).getMillis

  def getEventUTC(eventTime:Long, srcTZone:String):(Long, Int) = {
    srcTZone match {
      case "UTC" =>  (eventTime, 0)
      case "NOZONE" => (eventTime, 1)
      //case _ =>  (DateTimeZone.forID(srcTZone).convertLocalToUTC(eventTime, true), 0)
      case _ => {
        (eventTime, 0)
        //            unnecessary code. Conversion not required.
        //            could have merged UTC and others case but leaving it separate for clarity
        //            val dTzone = DateTimeZone.forID(srcTZone)
        //            val tzoneDate = new DateTime(eventTime, DateTimeZone.forID(srcTZone))
        //            (tzoneDate.plus(dTzone.getOffset(tzoneDate) * -1).toLocalDateTime.toDateTime(DateTimeZone.UTC).getMillis,0)
      }
    }
  }

  def buildMetaInfo(eventTs: Long, srcTZone: String, sourceHost: String, sourceName: String, batchId: String): MetaInfo = {
    val eventUtc = getEventUTC(eventTs, srcTZone)

    MetaInfo.newBuilder()
      .setEventUTC(eventUtc._1)
      .setSourceHost(sourceHost)
      .setSourceName(sourceName)
      .setBatchId(batchId)
      .setProcessingUTC(processingUTC)
      .setIsValidEventUTC(eventUtc._2)
      .build()
  }

  def apply(eventTs: Long, srcTZone: String, sourceHost: String, sourceName: String, batchId: String): MetaInfo = {
    buildMetaInfo(eventTs, srcTZone, sourceHost, sourceName, batchId)
  }
}
