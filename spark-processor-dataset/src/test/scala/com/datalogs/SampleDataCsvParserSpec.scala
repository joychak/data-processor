package com.datalogs

import com.datalogs.dataset.{SampleDatasetCsvEnricher, SampleDatasetCsvParser}
import org.scalatest._

class SampleDataCsvParserSpec extends FlatSpec with MustMatchers with EitherValues {

  "A SampleDataCsvParser" should "parse a valid data row" in {
    val ex = """12345","Field data11","Field data12","123451","Field data14","2016-04-25T21:58:21.000"""
    val recordObj = new LineRecord("filename", ex, "batchId", Map.empty[String, String])
    SampleDatasetCsvEnricher(SampleDatasetCsvParser(recordObj)).right.value mustBe a [RichRecord[_]]
  }
}
