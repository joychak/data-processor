package com.datalogs

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, LocalDate}

case class ProcessingFailure(error: Throwable, orig: Any)

/**
  * Standardize time formatting/reporting across Secured dataset
  *
  * @param utc
  */
case class DateTimeContext(utc: DateTime) {
  def utcString = DateTimeContext.printer.print(utc)
  //def localString = DateTimeContext.printer.print(local)
}
object DateTimeContext {
  private val printer = ISODateTimeFormat.dateTime()
}

case class LineRecord(fileName: String, line: String, batchId: String, refData:Map[String,String]) extends Serializable {
  override def toString: String = s"BatchId: $batchId:Source:$fileName, Record: $line"
}
case class RichRecord[T](record: T, ts: DateTimeContext) {
  def eventTime = ts.utc
  def transformedForWrite: (LocalDate, T) = (ts.utc.toLocalDate, record)
  def transformedForPartition: (DateTime, RichRecord[T]) = (this.eventTime, this)
}
case class PartialRecord[X](record:X, sourceName:String, batchId:String, hostName:String, refData:Map[String,String]) {
  def getRecord = record
  def getBatchId = batchId
  def getSourceName = sourceName
  def getHostName = hostName
  override def toString():String = s"BatchId: $batchId:Source:$sourceName:$record"
}
