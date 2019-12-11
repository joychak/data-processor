package com.datalogs

//import com.datalogs.sparkmetrics._
import com.datalogs.inputformats.{InputDataFormat, TextInputDataFormat}
import com.datalogs.resources.statestore.{BatchReprocessState, FileProcessState}
import com.datalogs.resources.{EncryptionInfo, HivePartitionRegistry, JobBatch, JobInput, JobOutput, JobPaths, JobSubmitConf, Trap}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, DateTimeZone, LocalDate}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

class BaseProcessor[T <: SpecificRecord : ClassTag](
    val register: Class[_], parser: BaseParser[T], enricher: BaseEnricher[T], thefilter: BaseFilter,
    inputDataType: InputDataFormat[InputFormat[LongWritable, Text]] = TextInputDataFormat()) extends Workflow with Logging {

  def getRefData(sc: SparkContext, refFileLoc:Option[Path]):Map[String, String] = Map.empty[String,String]

//  def runSingleBatch(sparkSession: SparkSession, opts: JobSubmitConf, batchId: String, encryptionInfo: EncryptionInfo, refData : Map[String, String], sparkJobMetricsPublisherContext : SparkJobMetricsPublisherContext) = {

  def runSingleBatch(sparkSession: SparkSession, opts: JobSubmitConf, batchId: String, encryptionInfo: EncryptionInfo, refData : Map[String, String]) = {

    logInfo(s"Starting BatchId: ${batchId} and interval: ${opts.duration()}")

    val paths = new JobPaths(opts, sparkSession.sparkContext.hadoopConfiguration)
    val batch = JobBatch.initBatch(batchId, opts.duration(), paths)
    val fileStateStore = new FileProcessState(sparkSession.sparkContext, opts.dataSourceName(), batch.batchId, batch.paths.opts.stateStoreDir().toString)
    val input = JobInput.prepare(sparkSession.sparkContext, batch, fileStateStore, opts.noop())
    if (input.hasValidInput) {
      val output = JobOutput.prepare[T](sparkSession.sparkContext, batch, opts.prepareNdays())
      val trap = Trap.prepare(sparkSession.sparkContext, batch)
      val hive = HivePartitionRegistry.prepare(sparkSession.sqlContext, opts.hive.get, output.removedPaths)

      // Counters are for the Parquet Conversion pipe-line only
//      val sparkJobCountersPublisher = new SparkJobCountersPublisher(sparkJobMetricsPublisherContext, sparkSession.sparkContext, trap.trapDir)

      val written = sparkSession.sparkContext
        .inputRecordSource(input.submitFilesString, batchId, refData, inputDataType, encryptionInfo)
        .trapToHdfs(trap, "badfile")
        .filter(thefilter)
        .parseData(parser)
        .enrichWith(enricher, output.validEventInterval)
        .trapToHdfs(trap, "badrec")
        .shuffleRandomlyAndSortByEventTime(opts.parallelism())
        .transformedForWrite()
        .saveOutput(output)
//      sparkJobCountersPublisher.publish(batchId)

      input.archiveInputFiles(sparkSession.sparkContext)
      hive.registerPartitionsInHive(written)
    } else {
      logInfo(s"BatchId ${batchId} is empty")
    }

    // Write the statestore ...
    fileStateStore.flush(input.submitFiles)
    // Write the _SUCCESS_BATCHID ...
    batch.commit()

    logInfo(s"Finished BatchId: ${batchId}")
  }

  def runBatches(sparkSession: SparkSession, opts: JobSubmitConf, thisBatchId: String) : Unit = {

    // Construct the objects that are common to all batches ...
    val encryptionInfo = new EncryptionInfo(opts)
    val refData = getRefData(sparkSession.sparkContext, opts.refDataDir.get)


//    val metPublisher = new MetricsPublisher(opts.publishMetrics(), opts.dataSourceName())
//    val sparkJobMetricsPublisherContext = SparkJobMetricsPublisherContext(metPublisher, opts.dataSourceName())
//    val sparkJobTimePublisher = new SparkJobTimePublisher(sparkJobMetricsPublisherContext)

    def run(currentBatchId: Option[String], brs: BatchReprocessState): Unit = {

      currentBatchId match {
        case Some(batchIdToRun) => {
          Try {
            // Reset the timer ...
//            sparkJobTimePublisher.reset
//            runSingleBatch(sparkSession, opts, batchIdToRun, encryptionInfo, refData, sparkJobMetricsPublisherContext)

            runSingleBatch(sparkSession, opts, batchIdToRun, encryptionInfo, refData)
            brs.markBatchSuccess(batchIdToRun)
          }.recover {
            case ex: Exception => {
              brs.markBatchFailure(batchIdToRun)
              logError(ex.getMessage, ex.getCause)
            }
          }

          // Time taken to run that batch (regardless of success/failure)
//          sparkJobTimePublisher.publish(batchIdToRun)

          // Run next batch (if available)
          val nextBatchId = brs.getNextBatchId
          run(nextBatchId, brs)
        }
        case _ =>
      }
    }

    //To run current + pending batches
//    val reprocessStateStore = BatchReprocessState.prepare(sparkSession.sparkContext, opts)
//    run(Some(thisBatchId), reprocessStateStore)
//    reprocessStateStore.flush

    runSingleBatch(sparkSession, opts, thisBatchId, encryptionInfo, refData)
  }

  def main(args: Array[String]): Unit = {

    val opts = new JobSubmitConf(args)
    val batchId = opts.batchId()

    val sparkConfigs = HashMap[String, String](
      "spark.serializer" -> classOf[KryoSerializer].getName,
      "spark.kryo.registrator" -> register.getName,
      "spark.driver.allowMultipleContexts" -> "true"
    )

    val sparkSession = DataLogsSparkSession.getOrCreate(configs = sparkConfigs, enableHiveSupport = opts.hive.isDefined)
    runBatches(sparkSession, opts, batchId)
  }
}

trait BaseFilter extends (LineRecord => Boolean) with Serializable {
  def applyFilter(recordObj:LineRecord):Boolean
  def apply(recordObj:LineRecord):Boolean= {
    applyFilter(recordObj)
  }
}
trait BaseParser[T] extends (LineRecord => Either[ProcessingFailure, PartialRecord[T]]) with Serializable {

  def getHostName(fileName: String):String = ""

  def getRecordObject(recordObj: LineRecord): T

  def getRefData(recordObj: LineRecord): Map[String, String] = recordObj.refData

  private def parseUnSafe(recordObj: LineRecord): Either[ProcessingFailure, PartialRecord[T]] = {
    try {
      Right(PartialRecord(getRecordObject(recordObj), recordObj.fileName, recordObj.batchId, getHostName(recordObj.fileName), getRefData(recordObj)))
    }
    catch {
      case NonFatal(e) => Left(ProcessingFailure(e, recordObj))
    }
  }

  def apply(recordObj: LineRecord): Either[ProcessingFailure, PartialRecord[T]] = {
    parseUnSafe(recordObj)
  }
}
trait BaseEnricher[T] extends (Either[ProcessingFailure, PartialRecord[T]] => Either[ProcessingFailure, RichRecord[T]]) with Serializable {

  def enrichWithMetadata(partialData:PartialRecord[T]):RichRecord[T] = {
    val dtc = DateTimeContext(new DateTime().withZone(DateTimeZone.UTC))
    RichRecord(partialData.getRecord, dtc)
  }

  def apply(pd: Either[ProcessingFailure, PartialRecord[T]]): Either[ProcessingFailure, RichRecord[T]] = {
    pd match {
      case Left(error) => Left(error)
      case Right(partialData) => try {
        Right (enrichWithMetadata(partialData))
      } catch {
        case NonFatal(e) => Left(ProcessingFailure(e, partialData))
      }
    }
  }
}

abstract class AvroSchemaRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[DateTime], JodaDateTimeSerializer)
    kryo.register(classOf[LocalDate], JodaLocalDateSerializer)
  }
}
object JodaDateTimeSerializer extends Serializer[DateTime] {
  override def read(kryo: Kryo, input: Input, `type`: Class[DateTime]): DateTime = new DateTime(input.readLong(), DateTimeZone.UTC)
  override def write(kryo: Kryo, output: Output, `object`: DateTime): Unit = output.writeLong(`object`.getMillis)
}
object JodaLocalDateSerializer extends Serializer[LocalDate] {
  override def read(kryo: Kryo, input: Input, `type`: Class[LocalDate]): LocalDate = new LocalDate(input.readInt(), input.readInt(), input.readInt())
  override def write(kryo: Kryo, output: Output, `object`: LocalDate): Unit = {
    output.writeInt(`object`.getYear)
    output.writeInt(`object`.getMonthOfYear)
    output.writeInt(`object`.getDayOfMonth)
  }
}