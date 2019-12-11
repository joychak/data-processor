package com.datalogs.resources.statestore

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.util.Random
import scala.util.control.NonFatal

class FileProcessState(@transient val sc : SparkContext,
                       datasource : String,
                       jobBatchId : String,
                       statestoreDirectory : String,
                       partFileSize : Int = 1000000) extends Serializable {

  def getBasename(s : String) : String = {
    val tokens = s.split('/')
    val tLen = tokens.length
    tokens(tLen - 1)
  }

  def getBasename(path: Path): String = getBasename(path.toString)

  val (successFileIsThere, dataFilesAreThere) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val ssPath = new Path(statestoreDirectory, "data")

    val logger = LoggerFactory.getLogger(this.getClass.getName)
    logger.info(s"Data Store path: ${ssPath.toString}")

    fs.exists(ssPath) match {
      case false => (false, false)
      case true =>
        (!fs.listStatus(ssPath).filter(_.getPath.getName == "_SUCCESS").isEmpty,
        !fs.listStatus(ssPath).filter(_.getPath.getName.startsWith("part")).isEmpty)
    }
         // StateStore was successfully written by previous batch !!!
  }

  require(successFileIsThere)

  val initialState = {
    if (dataFilesAreThere) {
      @transient lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
      sc
        .newAPIHadoopFile(s"${statestoreDirectory}/data/*", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
        .map {
          case (_, line) => {
            line.toString.split("""\|""") match {
              case Array(fileName, fileBatchId, remainder@_*) =>
                if (fileBatchId != jobBatchId) Some(getBasename(fileName), fileBatchId) else None
              case _ => {
                logger.warn(s"Failed to parse line ${line}")
                None
              }
            }
          }
        }
        .filter(_.isDefined)
        .map(_.get)
        .cache()
    }
    else {
      sc.emptyRDD[(String, String)]
    }
  }

  def getFilesForThisBatch(filesInBatchTimePeriod : Seq[String]) : (Seq[String], Seq[String]) = {

    val dupCheckResult = sc.parallelize(filesInBatchTimePeriod)
      .map(fileName => (getBasename(fileName), (fileName, jobBatchId)))
      .leftOuterJoin(initialState)
      .map(_  match {
        case (fileBaseName, ((fileFullName, jobBatchId), None)) => Left(fileFullName)
        case (fileBaseName, ((fileFullName, jobBatchId), Some(otherBatchId))) => Right(fileFullName)
      })
      .cache

    val process = dupCheckResult.filter(_.isLeft).map { case Left(fileFullName) => fileFullName }.collect()
    val duplicate = dupCheckResult.filter(_.isRight).map { case Right(fileFullName) => fileFullName }.collect()

    dupCheckResult.unpersist()
    (process, duplicate)
  }

  def flush(thisBatchFiles : Seq[Path]) = {

    // If the batch was empty, the statestore stays unaffected
    if (! thisBatchFiles.isEmpty) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val statestoreArchiveDirectory = new Path(statestoreDirectory, "ARCHIVES")
      val statestoreDataDirectory = new Path(statestoreDirectory, "data")

      // Write the new statestore contents in `temp` location
      val statestoreTempDirectory = new Path(statestoreDirectory, "temp")
      if (fs.exists(statestoreTempDirectory)) {
        fs.delete(statestoreTempDirectory, true)
      }

      val statestorePartitions = (((thisBatchFiles.size + initialState.count()) / partFileSize) + 1).toInt
      sc
        .parallelize(thisBatchFiles.map(getBasename(_)))
        .map(fileName => (fileName, jobBatchId))
        .union(initialState)
        .mapPartitions(partIter => {
          val r = new Random(System.currentTimeMillis.toInt)
          partIter.map {
            case (aFileName, aBatchId) => (r.nextInt(), s"${aFileName}|${aBatchId}|")
          }
        })
        .repartition(statestorePartitions)
        .values
        .saveAsTextFile(statestoreTempDirectory.toString)

      // If written successfully then :
      // 'data' -> 'archive' ≈
      if (fs.exists(statestoreArchiveDirectory))
        fs.delete(statestoreArchiveDirectory, true)
      fs.rename(statestoreDataDirectory, statestoreArchiveDirectory)
      // 'temp' -> 'data'
      if (fs.exists(statestoreDataDirectory))
        fs.delete(statestoreDataDirectory, true)
      fs.rename(statestoreTempDirectory, statestoreDataDirectory)
    }

    initialState.unpersist(false)
  }
}
