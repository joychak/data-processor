package com.datalogs

import com.datalogs.dataschema.{MetaInfo, SampleDatasetCsv}
import com.datalogs.dataset.SampleDatasetCsvEnricher
import org.apache.commons.math3.exception.OutOfRangeException
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Interval}
import org.scalatest._

import scala.util.Random

class SecuredAppSpec extends FlatSpec with Workflow with MustMatchers {
  "SecuredApp" should "partition evenly regardless of skew" in {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("name").setMaster("local[8]").set("spark.driver.allowMultipleContexts", "true"))

    val seed = 72481
    val random = new Random(seed)

    val recs = List.fill(100) {
      val dt = new DateTime(random.nextInt(60 * 60 * 24 * 7) * 1000L)
      RichRecord(new SampleDatasetCsv(0, "", "", 0, "", dt.toString(), new MetaInfo(0L,"","","", 0L, 0)), DateTimeContext(dt))
    } ++ List.fill(400) {
      val dt = new DateTime(2018, 5, 26, 0, 0) // add some serious skew to the dataset
      RichRecord(new SampleDatasetCsv(0, "", "", 0, "", dt.toString(),  new MetaInfo(0L,"","","", 0L, 0)), DateTimeContext(dt))
    }

    val evenlyDistributed = sc
      .parallelize(recs)
      .shuffleRandomlyAndSortByEventTime(10, seed)

    val sortCheck = evenlyDistributed.mapPartitions(partition => {
      Iterator( partition.sliding(2).forall{
        case first::second::Nil => first.ts.utc.getMillis <= second.ts.utc.getMillis
        case _ => false
      })
    }).distinct().collect() mustEqual(Array(true))

    val sizes = evenlyDistributed.mapPartitions(partition => Iterator(partition.toList.size.toDouble)).collect().toSeq

    val avg = sizes.sum / sizes.length
    val stddev = Math.sqrt(sizes.map { x => Math.pow(x - avg, 2) }.sum / sizes.length)

    println(s"sizes = ${sizes} avg = ${avg} stdev = ${stddev}")
    sizes.foreach(sz => sz mustEqual (avg +- 2*stddev))
  }


  it should "Trap records outside interval during enrich" in {

    val recs : List[Either[ProcessingFailure, PartialRecord[SampleDatasetCsv]]] = (0 until 5).map(ms => {
      val pr1 = new PartialRecord[SampleDatasetCsv](new SampleDatasetCsv(0, "", "", 0, "", (new DateTime(2018, 10, 1, 0, 0, 0, ms)).toString(), new MetaInfo(0L,"","","", 0L, 0)),"", "", "", Map.empty[String, String])
      Right(pr1)
    }).toList

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("name").setMaster("local[8]").set("spark.driver.allowMultipleContexts", "true"))

    val interval = new Interval(new DateTime(2018, 10, 1, 0, 0, 0, 1), new DateTime(2018, 10, 1, 0, 0, 0, 4))
    val results = sc.parallelize(recs).enrichWith(SampleDatasetCsvEnricher, interval).collect()

    val (pfs, rrs) = results.partition(res => res.isLeft)
    val allPfsAreOutOfRangeException = pfs.map(_.left.get).foldRight(true)((pf, acc) => acc & pf.error.isInstanceOf[OutOfRangeException])

    pfs.size == 2 &&
      rrs.size == 3 &&
      pfs.map(_.left.get.error.asInstanceOf[OutOfRangeException].getArgument.intValue()).toSet == Set(1, 2, 3)

  }
}
