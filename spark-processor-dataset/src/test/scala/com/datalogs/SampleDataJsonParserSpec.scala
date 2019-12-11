package com.datalogs

import com.datalogs.dataset.{SampleDatasetJsonEnricher, SampleDatasetJsonParser}
import org.scalatest._

import scala.io.{Source => SIOSource}

class SampleDatasetJsonParserSpec extends FlatSpec with MustMatchers with EitherValues {
  "A SampleDatasetParser" should "parse a good JSON string" in {
    val ex = """{"utc_datetime":"2018-04-25T21:58:21.000","id":"ABC1234","field1":10723484,"field2":1,"field3":0,"field4":0,"field5":0,"field6":208,"field7":""}"""
    val recordObj = new LineRecord("filename", ex, "batchId", Map.empty[String,String])
    SampleDatasetJsonEnricher(SampleDatasetJsonParser(recordObj)).right.value mustBe a [RichRecord[_]]
  }

  it should "generate Left(...) for bad JSON string" in {
    val ex = """{"utc_datetime":"2018-04-25T21:58:21.000","id":"ABC1234","field1":10723484,"field2":1,"field3":0,"field4":0,"field5":0,"field6":208,"field7":""}"""
    val recordObj = LineRecord("filename", ex, "batchId", Map.empty[String, String])
    SampleDatasetJsonEnricher(SampleDatasetJsonParser(recordObj)).left.value mustBe a [ProcessingFailure]
  }

  it should "parse a file of SampleDataset data" in {
    SIOSource
      .fromFile("spark-processor-dataset/data/SampleDataset.dat")
      .getLines()
      .map(x => LineRecord("filename", x.toString, "batchId", Map.empty[String,String]))
      .map(SampleDatasetJsonParser)
      .filter(_.isLeft).map(SampleDatasetJsonEnricher)
      .filter(_.isLeft) mustBe empty
  }
}