package com.datalogs.common

import com.datalogs.{BaseEnricher, BaseFilter, BaseParser, DateTimeContext, LineRecord, PartialRecord}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.joda.time.format.ISODateTimeFormat

import scala.reflect.ClassTag

object DatalogsFilter extends BaseFilter {
  override def applyFilter(recordObj:LineRecord):Boolean = true
}

abstract class DatalogsParser[T] (implicit tag: ClassTag[T]) extends BaseParser[T] {

  @transient lazy val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .setPropertyNamingStrategy(new DatalogsNamingStrategy(Map("utcDatetime" -> "utc_datetime", "id" -> "user_id", "userType" -> "user_type")))

  def getRecordString(recordObj: LineRecord): String = recordObj.line.toString


}

abstract class DatalogsEnricher[T](implicit tag: ClassTag[T]) extends BaseEnricher[T] {
  @transient lazy val df = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  def getUTCMillis(partialData:PartialRecord[T]): Long

  def getSourceTZone(partialData:PartialRecord[T]): String = "UTC"

  def getDTC(partialData:PartialRecord[T]): DateTimeContext


}
