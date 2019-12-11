package com.datalogs

import java.io.{File, PrintWriter}

import com.datalogs.resources.statestore.BatchReprocessState
import com.datalogs.resources.{JobBatch, JobSubmitConf}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.{Source => SIOSource}
import scala.reflect.io.Directory
import scala.util.Random
import org.scalatest._

import scala.collection.immutable.HashMap

/**
  * Created by user-1 on 11/8/2018.
  */


class BatchReprocessStateTest extends FlatSpec with Matchers {

  val datasourceName = "Dataset"
  def statestoreDirectory(testNum : Int) = s"target/reprocesstest/${testNum}"
  def generateArgs(batchId: String, testNum: Int, duration: Int) : Array[String] = {
    s"--input-dir dont-care --output-dir dont-care --archive-dir dont-care --trap-dir dont-care --state-store-dir ${statestoreDirectory(testNum)} --ref-data-dir dont-care --batch-id ${batchId} --duration ${duration} --hive dont.care --dataSourceName ${datasourceName}".split(" ")
  }

  val sc: SparkContext = DataLogsSparkSession.getOrCreate(
    master = "local",
    appName = "ReprocessStateStoreTest",
    configs = HashMap[String, String]("spark.driver.allowMultipleContexts" -> "true")
  ).sparkContext

  def writeFile(filename: String, contents: Array[String]) = {
    println(s"WriteFile ${filename} with contents ${contents.mkString("\n")}")
    val pw = new PrintWriter(new File(filename))
    contents.foreach(d => pw.write(s"${d}|*|\n"))
    pw.close()
  }

  def removeFile(filename: String) = {
    println(s"RemoveFile ${filename}")
    FileUtils.deleteQuietly(new File(filename))
  }

  def getToReprocessFilename(testNum: Int) : String = s"${statestoreDirectory(testNum)}/reprocess/${datasourceName}-to-reprocess.txt"
  def getAllFailedFilename(testNum: Int) : String = s"${statestoreDirectory(testNum)}/reprocess/${datasourceName}-failed-batches.txt"

  def setup(contents: Array[String], duration: Long, testNum: Int, filename: String = "") = {
    val dir = new Directory(new File(statestoreDirectory(testNum)))
    if (dir.exists)
      dir.deleteRecursively()
    dir.createDirectory()

    val subdir = new Directory(new File(statestoreDirectory(testNum), "reprocess"))
    subdir.createDirectory()

    writeFile(getToReprocessFilename(testNum), contents)
    writeFile(getAllFailedFilename(testNum), contents)

    val batchId =   DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC().print((new DateTime).getMillis)
    val jobConf = new JobSubmitConf(generateArgs(batchId, testNum, 0))
    def computeScheduledEndTimeTestFunc(jobConf: JobSubmitConf) : DateTime = JobBatch.getInterval(jobConf.batchId(), jobConf.duration()).getEnd.plus(duration)
    println(s"Preparing to run schedule batch with BatchId: ${batchId}")
    val rsst = BatchReprocessState.prepare(sc, jobConf, computeScheduledEndTimeTestFunc)

    rsst
  }

  val singleRow = Array[String]("20181030020000")
  val multipleRow = Array[String]("20181029020000", "20181030020000", "20181031020000")

  "BatchReprocessState" should "allow to run batches for scheduled duration" in {

    val rsst = setup(singleRow, 3000L, 1)
    val batchInfo = rsst.getNextBatchId()
    batchInfo.isEmpty should be (false)
  }

  it should "not allow to run batches after scheduled duration" in {

    val rsst = setup(multipleRow, 3000L, 2)
    rsst.getNextBatchId().isEmpty should be (false)
    Thread.sleep(3000L)
    rsst.getNextBatchId().isEmpty should be (true)
  }

  it should "Return correct reprocess batch(es) - when configured with only 1 batch" in {

    val rsst = setup(singleRow, 3000L, 3)

    val batchInfo = rsst.getNextBatchId()
    batchInfo.isEmpty should be (false)
    batchInfo.get should be ("20181030020000")

    rsst.getNextBatchId().isEmpty should be (true)
  }

  it should "Return correct reprocess batch(es) - when configured with multiple batches" in {

    val rsst = setup(multipleRow, 3000L, 4)

    def getAllBatchInfo(acc: List[String]): List[String] = {
      val batchInfoOrNone = rsst.getNextBatchId()
      batchInfoOrNone match {
        case None => acc
        case Some(batchId) => getAllBatchInfo(batchId :: acc)
      }
    }

    getAllBatchInfo(List.empty[String]).toSet shouldEqual (Set[String]("20181029020000", "20181030020000", "20181031020000"))
  }

  it should "Clear out Failed Batches file after successful run of all batches" in {

    val thisTestNum = 5
    val rsst = setup(multipleRow, 3000L, thisTestNum)

    SIOSource.fromFile(getAllFailedFilename(thisTestNum)).getLines().size shouldBe (multipleRow.size)

    def processAllBatches() : Unit = {
      rsst.getNextBatchId() match {
        case None => rsst.flush()
        case Some(batchId) => rsst.markBatchSuccess(batchId); processAllBatches()
      }
    }

    processAllBatches

    SIOSource.fromFile(getAllFailedFilename(thisTestNum)).getLines().size == 0

  }

  it should "Retain failed batches in the Failed Inventory file" in {
    val thisTestNum = 6
    val rsst = setup(multipleRow, 3000L, thisTestNum)
    var failedBatches = Set.empty[String]

    SIOSource.fromFile(getAllFailedFilename(thisTestNum)).getLines().size shouldBe (multipleRow.size)

    def processAllBatches() : Unit = {
      rsst.getNextBatchId() match {
        case None => rsst.flush()
        case Some(batchId) => {
          if (Random.nextBoolean() == false) {
            rsst.markBatchFailure(batchId)
            failedBatches += batchId
          }
          else
            rsst.markBatchSuccess(batchId)
          processAllBatches()
        }
      }
    }

    processAllBatches()

    SIOSource.fromFile(getAllFailedFilename(thisTestNum)).getLines().size shouldBe (failedBatches.size)
    SIOSource.fromFile(getAllFailedFilename(thisTestNum)).getLines()
      .map(_.split("""\|""")(0)).foldLeft(List[String]())((acc, str) => str :: acc).toSet shouldEqual(failedBatches)
  }
}
