package com.datalogs.resources

import java.io._

import com.datalogs.{Logging, ProcessingFailure, Utility}
import com.datalogs.outputformats.{DateAndBatchPartitionerFactory, ParquetRecordWriterFactory, PartitionRecordWriterFactory, PartitionedOutputFormat, PartitionedRecordWriterFactory}
import com.datalogs.resources.statestore.FileProcessState
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroWriteSupport
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SerializableWritable, SparkContext, TaskContext}
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.rogach.scallop.ScallopConf

import scala.reflect._
import scala.util.control.NonFatal

class JobSubmitConf(args: Array[String], rerunBatchId: String = "") extends ScallopConf(args) {

  private val df = ISODateTimeFormat.dateTimeParser()

  private def toHiveSetup(s: String) : HiveSetup = s.split("\\.") match {
    case Array(schema, table) => HiveSetup(schema, table)
  }

  val inputDir        = opt[String]("input-dir", required = true).map(new Path(_))
  val outputDir       = opt[String]("output-dir", required = true).map(new Path(_))
  val archiveDir      = opt[String]("archive-dir").map(new Path(_))
  val trapDir         = opt[String]("trap-dir").map(new Path(_))
  val stateStoreDir   = opt[String]("state-store-dir", required = true).map(new Path(_))
  val parallelism     = opt[Int]("parallelism", default = Some(20))
  val prepareNdays    = opt[Int]("prepare-n-days", default = Some(100))
  val refDataDir      = opt[String]("ref-data-dir").map(new Path(_))
  val batchId         = opt[String]("batch-id")
  val hive            = opt[String]("hive", descr = "[SCHEMA].[TABLE] to register partitions into").map(toHiveSetup)
  val noop            = opt[Boolean]("noop", default = Some(false))
  val publishMetrics  = opt[Boolean]("publishMetrics", default = Some(false))
  val dataSourceName  = opt[String]("dataSourceName", default = Some("Default"))
  val duration        = opt[Int]("duration", default = Some(120))
  val privateKeyFile  = opt[String]("private-key-file")

  /** Obsolete **/
  @deprecated("startDate is no longer part of commandline parameter","Code refactoring") val startDate = opt[String]("start-date").map(df.parseDateTime)
  @deprecated("startDate is no longer part of commandline parameter","Code refactoring") val endDate   = opt[String]("end-date").map(df.parseDateTime)

  verify()

  val reprocessStoreDir     = new Path(stateStoreDir().toString, "reprocess")
}

case class EncryptionInfo(opts: JobSubmitConf) extends Logging {

  val (enabled, privateKeyFile) = {
    opts.privateKeyFile.get match {
      case None => (false, null)
      case Some(s) => (true, s)
    }
  }

  logInfo(s"EncryptionInfo : Enabled = ${enabled}, PrivateKey = ${privateKeyFile} ")
}

case class JobPaths(opts: JobSubmitConf, conf: Configuration) {
  def inputFs: FileSystem = opts.inputDir().getFileSystem(conf)
  def outputFs: FileSystem = opts.outputDir().getFileSystem(conf)
  def archiveFs: Option[FileSystem] = opts.archiveDir.get.map(_.getFileSystem(conf))
  def trapFs: FileSystem = opts.outputDir().getFileSystem(conf)
  def stateStoreFs: FileSystem = opts.stateStoreDir().getFileSystem(conf)
}

case class JobBatch private(batchId: String, paths: JobPaths, dates: Interval, partitions: (LocalDate, String) => String) {
  def commit(): Unit = {
    val (fs, successFilePath) = JobBatch.markerPath(batchId, paths)
    fs.create(successFilePath).close()
  }
}
object JobBatch extends Utility with Logging {
  private def markerPath(batchId: String, paths: JobPaths): (FileSystem, Path) = {
    (paths.outputFs, paths.opts.outputDir() / s"_SUCCESS_${batchId}")
  }

  def getInterval(batchId: String, duration: Int) : Interval = {
    val utcBatchId = batchId + "UTC"
    val endDate = DateTimeFormat.forPattern("yyyyMMddHHmmssz").parseDateTime(utcBatchId)
    new Interval(endDate.minusMinutes(duration), endDate)
  }

  def initBatch(batchId: String, duration: Int, paths: JobPaths): JobBatch = {
    val (fs, successFilePath) = markerPath(batchId, paths)
    if (fs.exists(successFilePath) && fs.isFile(successFilePath)) fs.delete(successFilePath, false)

    val dates = getInterval(batchId, duration)
    logInfo(s"Interval start: ${dates.getStartMillis} and end: ${dates.getEndMillis}")

    val partitions = (l: LocalDate, b: String) => DateAndBatchPartitionerFactory.assignPartition(l, b)

    new JobBatch(batchId, paths, dates, partitions)
  }
}

class JobInput private(batchId: String, paths: JobPaths, files: Seq[JobInput.Incoming], noop: Boolean) extends Logging with Utility {

  val fs = paths.opts.inputDir().getFileSystem(paths.conf)

  lazy val submitFiles = files.collect {
    case JobInput.Process(p) => p
  }

  lazy val duplicateFiles = files.collect {
    case JobInput.Duplicate(p) => p
  }

  val hasValidInput = if (submitFiles.length == 0) {
    duplicateFiles.foreach(JobInput.delete(_, fs))
    false
    //Creating the final _SUCCESS file to mark the spark job'c completion status
    //SecuredUtility.createSuccessFile(outputDir, sc, batchId)

    //throw new Exception("No file is selected for processing")
  } else {
    true
  }

  private def archiveFiles(sc : SparkContext, archiveDirectory: Path): Unit = {

    if (!fs.exists(archiveDirectory) || !fs.isDirectory(archiveDirectory))
      fs.mkdirs(archiveDirectory)

    logInfo("Archiving files ...")
    val archiveOperationPartitions = (submitFiles.size / JobInput.HDFS_MOVE_PARTITION_SIZE + 1)
    archiveOperationPartitions match {
      case 1 => submitFiles.foreach(JobInput.moveFiles(_, archiveDirectory, fs))
      case _ => {
        val hadoopConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))
        val archiveDirectoryLocation = sc.broadcast(archiveDirectory.toString)

        sc.parallelize(submitFiles, archiveOperationPartitions)
          .foreachPartition(thisPartitionFilesIterator => {
            thisPartitionFilesIterator.foreach(singleFilePath => {
              val pfs = FileSystem.get(hadoopConf.value.value)
              val destPath = new Path(s"${archiveDirectoryLocation.value}/${singleFilePath.getName}")
              pfs.rename(singleFilePath, destPath)
            })
          })
      }
    }
    logInfo("Archiving files ... done")

    logInfo("Deleting duplicate files ...")
    val deleteOperationPartitions = (duplicateFiles.size / JobInput.HDFS_DELETE_PARTITION_SIZE + 1)
    deleteOperationPartitions match {
      case 1 => duplicateFiles.foreach(JobInput.delete(_, fs))
      case _ => {
        val hadoopConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

        sc.parallelize(duplicateFiles, deleteOperationPartitions)
          .foreachPartition(thisPartitionFilesIterator => {
            val pfs = FileSystem.get(hadoopConf.value.value)
            thisPartitionFilesIterator.foreach(pfs.delete(_, false))
          })
      }
    }
    logInfo("Deleting duplicate files ... done")

    val potentiallyEmptyDirs =
      fs
        .listStatus(paths.opts.inputDir())                                // List contents of input directory
        .filter(fileStatus => fileStatus.isDirectory)                     // Retain only directories
        .filter(fileStatus => fs.listStatus(fileStatus.getPath).isEmpty)  // Retain directories that are empty
        .map(_.getPath)

    potentiallyEmptyDirs.foreach(path => {
      logInfo(s"Clearing empty incoming subdirectory ${path.toString}")
      // Intentionally not deleting recursively. Its very possible that new files have come in
      // this sub-directory since last time we found it to be empty !!!
      try {
        fs.delete(path, false)
      }
      catch {
        case NonFatal(ioe) => logWarning(s"Failed to delete ${path.toString}, it may not be empty anymore")
      }
    })
  }

  def archiveInputFiles(sc : SparkContext): Unit = {

    val archiveDir = paths.archiveFs
    archiveDir match {
      case Some(archiveDir) if !noop => {
        archiveFiles(sc, paths.opts.archiveDir()/ batchId)
      }
      case None if !noop => {
        //cleanUpFiles.foreach(delete)
        archiveFiles(sc, paths.opts.archiveDir() / "ARCHIVES" / batchId)   //Move the file to a default location ARCHIVES/batch-id under output (if no archive location specified)
      }
      case _ => // do nothing
    }
  }

  def submitFilesString: String = submitFiles.mkString(",")
}
object JobInput extends Logging with Utility {

  private val HDFS_MOVE_PARTITION_SIZE = 100
  private val HDFS_DELETE_PARTITION_SIZE = 100

  private sealed trait Incoming {
    def path: Path
  }
  private case class Process(path: Path) extends Incoming
  private case class Duplicate(path: Path) extends Incoming

  // FIXME - shouldn't rely on "rename" (not availble on S3) - switch to copy/delete?
  private def moveFiles(srcFilePath: Path, destDir: Path, fs: FileSystem): Unit = fs.rename(srcFilePath, destDir / srcFilePath.getName)
  private def delete(p: Path, fs: FileSystem): Unit = fs.delete(p, false)

  private def moveFilesFromArchiveToInput(sc: SparkContext, archiveDir: Path, batch: JobBatch): Unit = {
    if (!archiveDir.toString.contains(batch.batchId)) return

    val fs = batch.paths.inputFs

    if (fs.exists(archiveDir) && fs.isDirectory(archiveDir)) {
      val archivedFiles = fs.listStatus(archiveDir).map(_.getPath)
      val archiveMovePartitions = (archivedFiles.size / HDFS_MOVE_PARTITION_SIZE + 1)
      logInfo("Moving files from Archive to Input ...")
      archiveMovePartitions match {
        case 1 => archivedFiles.foreach{srcPath => moveFiles(srcPath, batch.paths.opts.inputDir(), fs)}
        case _ => {
          val hadoopConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))
          val inputDirectoryLocation = sc.broadcast(batch.paths.opts.inputDir().toString)

          sc.parallelize(archivedFiles, archiveMovePartitions)
            .foreachPartition(thisPartitionFilesIterator => {
              val pfs = FileSystem.get(hadoopConf.value.value)
              thisPartitionFilesIterator.foreach(singleFilePath => {
                val destPath = new Path(s"${inputDirectoryLocation.value}/${singleFilePath.getName}")
                pfs.rename(singleFilePath, destPath)
              })
            })
        }
      }
      logInfo("Moving files from Archive to Input ... done")

      fs.listStatus(archiveDir).foreach{file => moveFiles(file.getPath, batch.paths.opts.inputDir(), fs)}
      fs.delete(archiveDir, true)
    }
  }
  private def cleanArchiveDir(sc: SparkContext, batch: JobBatch, noop: Boolean) : Unit = {

    val archiveDir = batch.paths.archiveFs
    archiveDir match {
      case Some(archiveDir) if !noop => {
        moveFilesFromArchiveToInput(sc, batch.paths.opts.archiveDir()/ batch.batchId, batch)
      }
      case None if !noop => {
        moveFilesFromArchiveToInput(sc, batch.paths.opts.outputDir() / "ARCHIVES" / batch.batchId, batch)   //Move the file to a default location ARCHIVES/batch-id under output (if no archive location specified)
      }
      case _ => // do nothing
    }
  }

  def prepare(sc: SparkContext, batch: JobBatch, stateStore: FileProcessState, noop: Boolean): JobInput = {
    logInfo("***********")
    logInfo(s"Checking presence of archive file directory and all the files inside it")
    cleanArchiveDir(sc, batch, noop)

    logInfo("***********")

    logInfo(s"Evaluating ${batch.paths.opts.inputDir()} to find files to process")
    val fs = batch.paths.inputFs
    val filesInTimeInterval = fs.listStatus(batch.paths.opts.inputDir())
      .flatMap(stat => {
        stat.isDirectory match {
          case true => fs.listStatus(stat.getPath)
          case false => Seq(stat)
        }
      })
      .filter { stat =>
        val r = !stat.getPath.getName.endsWith(".filepart") && batch.dates.contains(stat.getModificationTime)
        logInfo(s"Checking if ${stat.getPath} is in range of ${batch.dates.getStart} to ${batch.dates.getEnd}: ${r}")
        r
      }
      .map(_.getPath.toString)

    val (processFiles, duplicateFiles) = stateStore.getFilesForThisBatch(filesInTimeInterval)
    val jobInputFiles = processFiles.map(p => Process(new Path( p))) ++ duplicateFiles.map(d => Duplicate(new Path(d)))

    logInfo(s"""files = ${jobInputFiles.mkString(",\n")}""")
    logInfo("***********")

    new JobInput(batch.batchId, batch.paths, jobInputFiles, noop)
  }
}

class JobOutput(batchId: String, dates: Interval, ndays: Int, paths: JobPaths, partitions: (LocalDate, String) => String, val removedPaths : List[Path]) extends Utility{

  private def searchForWrittenData(): List[Path] = {
    val fs = paths.outputFs

    Iterator.iterate(dates.getEnd.toLocalDate)(_.minusDays(1))
      .take(ndays + Days.daysIn(dates).getDays)
      .map { eventDate =>
        paths.opts.outputDir() / partitions(eventDate, batchId)
      }
      .collect {
        case p if fs.exists(p) && fs.isDirectory(p) => p
      }.toList
  }

  def validEventInterval = new Interval(dates.getEnd.minusDays(ndays).getMillis, dates.getEndMillis, DateTimeZone.UTC)

  def saveToSink[V : ClassTag](rdd: RDD[(LocalDate, V)]): List[Path] = {
    rdd.saveAsHadoopFile(paths.opts.outputDir().toString, classOf[LocalDate], classTag[V].runtimeClass, classOf[PartitionedOutputFormat[LocalDate, V]])
    searchForWrittenData()
  }
}
object JobOutput extends Logging with Utility {
  /**
    * Recovery of a failed batch means deleted all yyyy-mm-dd/batch_id folders... BUT since we don't know a priori
    * which event dates will contain this batch, in the absence of a secondary index we'd need to do an exhaustive
    * search throuhg ALL dates... instead we use a simple heuristic and look in the past `n` days
    */
  private def cleanAnyPreviousSameBatchOutput(batch: JobBatch, ndays: Int): List[Path] = {

    val fs = batch.paths.outputFs
    val init = DateTimeFormat.forPattern("yyyyMMdd").parseLocalDate(batch.batchId.substring(0, 8))

    val allDirs = Iterator.iterate(init)(_.minusDays(1))
      .take(ndays)
      .map { eventDate =>
        batch.paths.opts.outputDir() / batch.partitions(eventDate, batch.batchId)
      }
      .map { p =>
        if (fs.exists(p) && fs.isDirectory(p)) {
          Left(p)
        } else {
          Right(p)
        }
      }.toList

    val dirToRemove = allDirs.filter {
      case Left(p) => {
        logInfo(s"Deleting output path ${p}")
        fs.delete(p, true)
        true
      }
      case Right(p) => {
        logWarning(s"Skipping non-existent output path ${p}")
        false
      }
    }.map(_.left.get)


    val successFilePath = batch.paths.opts.outputDir() / s"_SUCCESS_${batch.batchId}"
    if (fs.exists(successFilePath) && fs.isFile(successFilePath)) fs.delete(successFilePath, false)

    dirToRemove
  }

  private def setAvroWriter[V <: SpecificRecord : ClassTag, RWF <: PartitionRecordWriterFactory[LocalDate, V] : ClassTag](sc: SparkContext, batchId: String) = {

    AvroWriteSupport.setSchema(sc.hadoopConfiguration, SpecificData.get().getSchema(classTag[V].runtimeClass))
    PartitionedRecordWriterFactory.setRecordWriterFactory[RWF](sc.hadoopConfiguration)
    DateAndBatchPartitionerFactory.setPartitionerFactory(sc.hadoopConfiguration, batchId)
  }

  def prepare[V <: SpecificRecord](sc: SparkContext, batch: JobBatch, ndays: Int)
                                             (implicit ct: ClassTag[V], ct2: ClassTag[PartitionRecordWriterFactory[LocalDate, V]]): JobOutput = {

    val removedPaths = cleanAnyPreviousSameBatchOutput(batch, ndays)

    DeprecatedParquetOutputFormat.setCompression(sc.hadoopConfiguration, CompressionCodecName.SNAPPY)
    DeprecatedParquetOutputFormat.setWriteSupportClass(sc.hadoopConfiguration, classOf[AvroWriteSupport[V]])
    DeprecatedParquetOutputFormat.setBlockSize(sc.hadoopConfiguration, 256 * 1024 * 1024)

    setAvroWriter[V, ParquetRecordWriterFactory[LocalDate, V]](sc, batch.batchId)

    new JobOutput(batch.batchId, batch.dates, ndays, batch.paths, batch.partitions, removedPaths)
  }

  //  def avroSpecificRecord[V <: SpecificRecord : ClassTag](sc: SparkContext, outputDir: Path, batchId: String): Sink[(LocalDate, V)] = {
  //    avroLikeSink[V, AvroSpecificRecordRecordWriterFactory[LocalDate, V]](sc, outputDir, batchId)
  //  }
}

class Trap(val trapDir: String, confBcast: Broadcast[SerializableWritable[Configuration]]) extends Serializable with Logging {

  def trap[T : ClassTag](rdd: RDD[Either[ProcessingFailure, T]], trapType: String): RDD[T] = {

    rdd.mapPartitions { iter =>
      val ctxt = TaskContext.get()
      val p = new Path(trapDir, s"trap-$trapType-%s".format(ctxt.taskAttemptId()))

      logInfo(s"Opening trap for context ${ctxt} at path ${p}")

      new Iterator[T] {
        lazy val writer = {
          val fs = p.getFileSystem(confBcast.value.value)
          val out = fs.create(p)
          new PrintWriter(new OutputStreamWriter(out))
        }

        var written = false

        val it = iter.filter {
          case Left(e) => {
            try {
              writer.println(e)
              written = true
              if (isDebugEnabled && e.isInstanceOf[ProcessingFailure])
                writer.println(s"Stack Trace: " + e.asInstanceOf[ProcessingFailure].error.getStackTrace)
            } catch {
              case NonFatal(e) => logError(s"Error writing trap ${writer}", e)
            }
            false
          }
          case Right(_) => true
        }.map(_.right.get)

        override def next(): T = it.next()

        override def hasNext: Boolean = {
          if (it.hasNext) {
            true
          } else {
            if (written) writer.close()
            false
          }
        }
      }
    }
  }
}
object Trap extends Logging with Utility {

  def prepare(sc: SparkContext, batch: JobBatch): Trap = {
    val fs = batch.paths.trapFs

    val trapDir = batch.paths.opts.trapDir() / batch.batchId
    if (fs.exists(trapDir)) {
      logInfo(s"Found existing batchDir ${trapDir}... will delete and recreate")
      if (!fs.delete(trapDir, true) && fs.mkdirs(trapDir))
        throw new IllegalStateException("Creation of directory failed " + trapDir)
    } else {
      if (!fs.mkdirs(trapDir))
        throw new IllegalStateException(s"Creation of batchDir `${trapDir}` failed")
    }

    new Trap(trapDir.toString, sc.broadcast(new SerializableWritable(sc.hadoopConfiguration)))
  }
}

case class HiveSetup(schema: String, table: String)
class HivePartitionRegistry private (sqlContext : SQLContext, hiveSetup: Option[HiveSetup], removedPaths: List[Path]) extends Logging {

  hiveSetup match {
    case None => //
    case Some(hiveSetup) => {
      removedPaths.foreach { path =>
        val pstring = createPartitionString(path)

        val sql = s"ALTER TABLE ${hiveSetup.schema}.${hiveSetup.table} DROP IF EXISTS PARTITION (${pstring})"
        logInfo(s"Removing Hive partition: ${sql}")
        sqlContext.sql(sql)
      }
    }
  }

  private def createPartitionString(path: Path): String = {
    path.toString.split("/").map(_.split("=")).foldLeft(Seq.empty[String]) {
      case (s, Array(p, v)) => s ++ Seq(s"${p}='${v}'")
      case (s, _) => s
    }.mkString(", ")
  }

  def registerPartitionsInHive(writtenData: List[Path]): Unit = {
    hiveSetup match {
      case None => //
      case Some(hiveSetup) => {
        writtenData.foreach { path =>
          val pstring = createPartitionString(path)

          val sql = s"ALTER TABLE ${hiveSetup.schema}.${hiveSetup.table} ADD IF NOT EXISTS PARTITION (${pstring}) LOCATION '${path.toString}'"
          logInfo(s"Updating Hive partition: ${sql}")
          sqlContext.sql(sql)
        }
      }
    }
  }
}
object HivePartitionRegistry {

  def prepare(sqlContext: SQLContext, hiveSetup: Option[HiveSetup], removedPaths: List[Path]): HivePartitionRegistry = {
    new HivePartitionRegistry(sqlContext, hiveSetup, removedPaths)
  }
}











