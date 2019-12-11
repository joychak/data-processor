package com.datalogs.common

import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}

object Utils {
  def getCurrentTimeinUtc:Long = {
    val ldatetime:LocalDateTime = new LocalDateTime()
    val utcDateTime:DateTime = ldatetime.toDateTime(DateTimeZone.UTC)
    utcDateTime.getMillis
  }
}