package com.datalogs

import java.io.PrintWriter

import com.datalogs.resources.statestore.FileProcessState
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.immutable.HashMap
import scala.io.{Source => SIOSource}
import scala.util.Random

/**
  * Created by user-1 on 3/15/17.
  */
class FileProcessStateTest extends FlatSpec with Matchers {

  val sc: SparkContext = DataLogsSparkSession.getOrCreate(
    master = "local",
    appName = "ReprocessStateStoreTest",
    configs = HashMap[String, String]("spark.driver.allowMultipleContexts" -> "true")
  ).sparkContext

  val fs = FileSystem.getLocal(new Configuration)
  fs.setVerifyChecksum(false)
  val testDir = "target/statestore-test"
  fs.delete(new Path(testDir), true)

  type StateStoreEntry = (String, String)
  type StateStoreTestReturn = (Seq[String], Seq[String], Seq[StateStoreEntry]) // ProcessFiles, DuplicateFiles, FinalStateStore

  def dirName(testName : String) = s"${testDir}/${testName}"
  def dataDirName(testName : String) = new Path(s"${dirName(testName)}/data")
  def archiveDirName(testName : String) = new Path(s"${dirName(testName)}/ARCHIVES")

  def stringToStateStoreEntry(s : String) = { val ss = s.split("""\|"""); (ss(0), ss(1)) }
  def stateStoreEntryToString(s : StateStoreEntry) = { s"${s._1}|${s._2}|"}
  def basename(s : String) = s.split("/").last

  def writeStateStoreFile(dirName : String, entries : Seq[StateStoreEntry]) = {

    // Intentionally create multiple files
    (0 until entries.size).foreach(partnum => {
      val pw = new PrintWriter(fs.create(new Path(s"${dirName}/part-${partnum}")))
      pw.println(stateStoreEntryToString(entries(partnum)))
      pw.close()
    })

    fs.create(new Path(s"${dirName}/_SUCCESS"))
  }

  def setupAndRun(testBatchId : String,
                  testName : String,
                  initialState : Seq[StateStoreEntry],
                  testBatchFiles : Seq[String]) : StateStoreTestReturn = {

    val ssDirectory = new Path(dirName(testName))
    if (fs.exists(ssDirectory)) fs.delete(ssDirectory, true)

    val dataDirectory = dataDirName(testName)
    if (!fs.exists(dataDirectory)) fs.mkdirs(dataDirectory)

    writeStateStoreFile(dataDirectory.toString, initialState)

    val ss = new FileProcessState(sc, "test", testBatchId, ssDirectory.toString, 1)
    val (processFiles, duplicateFiles) = ss.getFilesForThisBatch(testBatchFiles)
    ss.flush(processFiles.map(new Path(_)))

    val expected = (initialState.filter(_._2 != testBatchId) ++ processFiles.map((_, testBatchId)))
    (processFiles.sorted, duplicateFiles.sorted, expected.map(e => (basename(e._1), e._2)).sorted)
  }

  def actualContents(directory : Path) : Seq[String] = {
    if (fs.exists(directory))
      fs.listStatus(directory)
        .map(_.getPath.toUri)
        .map(filepath => SIOSource.fromFile(filepath).getLines())
        .filter(!_.isEmpty)
        .flatMap(_.toList)
    else
      Seq.empty[String]
  }

  def archiveContents(testname : String) : Seq[String] = actualContents(archiveDirName(testname))

  def statestoreContents(testname : String) : Seq[String] = actualContents(dataDirName(testname))

  it should "work for empty batch & empty statestore" in {
    val testName = "empty-batch-empty-statestore"
    val (p, d, e) = setupAndRun("0", testName, Seq.empty[StateStoreEntry], Seq.empty[String])
    p shouldBe Seq.empty[String]
    d shouldBe Seq.empty[String]
    e shouldBe Seq.empty[StateStoreEntry]

    statestoreContents(testName).toList.size shouldBe 0
  }

  it should "work for empty batch & non-empty statestore" in {
    val testName = "empty-batch-nonempty-statestore"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1")).sorted
    val (p, d, e) = setupAndRun("0", testName, initialState, Seq.empty[String])
    p shouldBe Seq.empty[String]
    d shouldBe Seq.empty[String]

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "work for nonempty batch & non-empty statestore, with no duplicates" in {
    val testName = "nonempty-batch-nonempty-statestore-no-duplicates"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1")).sorted
    val thisBatchFiles = Seq("f3", "f4")
    val (p, d, e) = setupAndRun("0", testName, initialState, thisBatchFiles)
    p shouldBe thisBatchFiles
    d shouldBe Seq.empty[String]

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }


  it should "work for nonempty batch & non-empty statestore, with some duplicates" in {
    val testName = "nonempty-batch-nonempty-statestore-some-duplicates"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1")).sorted
    val uniqueFiles = Seq("f3", "f4")
    val duplicateFiles = Seq("f1")
    val (p, d, e) = setupAndRun("0", testName, initialState, uniqueFiles ++ duplicateFiles)
    p shouldBe uniqueFiles
    d shouldBe duplicateFiles

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "work for nonempty batch & non-empty statestore, with all duplicates" in {
    val testName = "nonempty-batch-nonempty-statestore-all-duplicates"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1")).sorted
    val thisBatchFiles = Seq("f1", "f2")
    val (p, d, e) = setupAndRun("0", testName, initialState, thisBatchFiles)
    p shouldBe Seq.empty[String]
    d shouldBe thisBatchFiles

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "work when a batch is re-run with all files in that batch" in {

    val testName = "batch-rerun-all-files"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1"), ("f3", "2"), ("f4", "2"), ("f5", "3")).sorted
    val thisBatchFiles = Seq("f3", "f4")
    val (p, d, e) = setupAndRun("2", testName, initialState, thisBatchFiles)
    p shouldBe thisBatchFiles
    d shouldBe List.empty[String]

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "work when a batch is re-run with NOT all files in that batch" in {
    val testName = "batch-rerun-some-files"
    val initialState = Seq[StateStoreEntry](("f1","1"), ("f2", "1"), ("f3", "2"), ("f4", "2"), ("f5", "3")).sorted
    val thisBatchFiles = Seq("f3")
    val (p, d, e) = setupAndRun("2", testName, initialState, thisBatchFiles)
    p shouldBe thisBatchFiles
    d shouldBe Seq.empty[String]

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "work when statestore and inputs have fullpath instead of basename" in {

    val testName = "statestore-with-fullpath"
    val initialState = Seq[StateStoreEntry](("incoming/f1","1"), ("incoming/f2", "2"))
    val thisBatchFiles = Seq("archive/f1")
    val (p, d, e) = setupAndRun("1", testName, initialState, thisBatchFiles)
    p shouldBe thisBatchFiles
    d shouldBe Seq.empty[String]

    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe e
  }

  it should "retain a backup of current statestore" in {

    val testName = "statestore-backup-test"

    val testDir = new Path(dirName(testName))
    if (fs.exists(testDir)) {
      fs.delete(testDir, true)
    }

    fs.mkdirs(testDir)
    fs.create(new Path(dataDirName(testName), "_SUCCESS"))

    def singleBatchContents(batchId : String) : (String, String) = (s"f${batchId}", batchId)
    def expectedArchiveContents(batchId : String) = (1 to batchId.toInt - 1).map(ii => singleBatchContents(ii.toString)).toList.sorted
    def expectedDataContents(batchId : String) = (1 to batchId.toInt).map(ii => singleBatchContents(ii.toString)).toList.sorted

    def runBatch(batchIdStr : String) = {
      val ss = new FileProcessState(sc, "test", batchIdStr, testDir.toString, 1)
      val (p, _) = ss.getFilesForThisBatch(Seq[String](s"f${batchIdStr}"))
      ss.flush(p.map(new Path(_)))

      archiveContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe expectedArchiveContents(batchIdStr)
      statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe expectedDataContents(batchIdStr)
    }

    (1 to 4).foreach(ii => runBatch(ii.toString))
  }

  it should "evenly distributed statestore data in files" in {
    val testName = "even-distribution-test"

    val testDir = new Path(dirName(testName))
    if (fs.exists(testDir)) {
      fs.delete(testDir, true)
    }

    fs.mkdirs(testDir)
    fs.create(new Path(dataDirName(testName), "_SUCCESS"))

    val files = (1 to (Random.nextInt(5000) + 1)).map(ii => s"input_${ii}.inp")
    val ss = new FileProcessState(sc, "test", "1", testDir.toString, 73)
    ss.flush(files.map(new Path(_)))

    val partFiles = fs.listStatus(dataDirName(testName)).map(_.getPath.toUri).filter(_.toString == "_SUCCESS")
    partFiles.foreach(partFile => {
      val lines = SIOSource.fromFile(partFile).getLines().size
      val withinBounds = (lines >= 70 && lines <=76)
      if (!withinBounds)
        println(s"Check file ${partFile}")
      withinBounds shouldBe true
    })
  }

  it should "fail if the statestore dir is in bad state" in {
    val testName = "bad-statestore-dir"

    val testDir = new Path(dirName(testName))
    if (fs.exists(testDir)) {
      fs.delete(testDir, true)
    }

    fs.mkdirs(testDir)
    // DO NOT create the _SUCCESS file
    // fs.create(new Path(dataDirName(testName), "_SUCCESS"))

    intercept[IllegalArgumentException] {
      val ss = new FileProcessState(sc, "test", "1", testDir.toString)
    }
    // If exception is not throw, this test will actually fail
  }

  it should "retain the archive & data when writing new statestore fails" in {
    val testName = "statestore-write-fail"

    val testDir = new Path(dirName(testName))
    if (fs.exists(testDir)) {
      fs.delete(testDir, true)
    }

    fs.mkdirs(testDir)
    fs.create(new Path(dataDirName(testName), "_SUCCESS"))
    writeStateStoreFile(dataDirName(testName).toString, Seq[StateStoreEntry](("f1","1")))
    fs.rename(dataDirName(testName), new Path(testDir, "ARCHIVES"))


    fs.create(new Path(dataDirName(testName), "_SUCCESS"))
    writeStateStoreFile(dataDirName(testName).toString, Seq[StateStoreEntry](("f1","1"), ("f2","2")))

    val ss = new FileProcessState(sc, "test", "3", testDir.toString)
    val (p, _) = ss.getFilesForThisBatch(Seq[String]("f3", "f4"))

    // Cause the RDD to be evaluated
    ss.initialState.count()

    // Intentionally cause something terrible
    ss.initialState.partitions.zipWithIndex.foreach{
      case (p : Partition, i : Int) => ss.initialState.partitions.update(i, null)
    }

    // Flush the statestore, should crash
    intercept[Exception] {
      ss.flush(p.map(new Path(_)))
    }

    archiveContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe Seq[StateStoreEntry](("f1","1"))
    statestoreContents(testName).map(line => stringToStateStoreEntry(line)).toList.sorted shouldBe Seq[StateStoreEntry](("f1","1"), ("f2","2"))
  }
}
