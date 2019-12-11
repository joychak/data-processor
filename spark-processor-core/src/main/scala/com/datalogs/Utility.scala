package com.datalogs

import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, LocalDate}

trait Utility {
  implicit class PathOps(val p: Path) {
    def /(s: String): Path = new Path(p, s)
  }

  implicit class StringPathOps(val s: String) {
    def /(z: String): String = new Path(s, z).toString
  }

  implicit val jodaDateTimeOrdering = Ordering.fromLessThan[DateTime](_ isBefore _)
  implicit val jodaLocalDateOrdering = Ordering.fromLessThan[LocalDate](_ isBefore _)
}