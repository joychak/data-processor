package com.datalogs

import com.databricks.spark.xml.XmlInputFormat
import com.datalogs.inputformats.{BadXMLInputDataFormat, BadXmlInputFormat, EncryptedTextInputFormat, FailSafeInputFormat, InputDataFormat, TextInputDataFormat, XMLInputDataFormat}
import com.datalogs.resources.{EncryptionInfo, JobOutput, Trap}
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.math3.exception.OutOfRangeException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.joda.time.{Interval, LocalDate}

import scala.reflect._
import scala.util.Random

trait Workflow {
  implicit class SecuredSparkContext(sc: SparkContext) {

    def inputRecordSource[I <: InputFormat[LongWritable, Text] : ClassTag](paths: String, batchId: String, refData: Map[String, String], inputDataType: InputDataFormat[I], encryptionInfo: EncryptionInfo)
          : RDD[Either[ProcessingFailure, LineRecord]] = {

      val batchIdBcast = sc.broadcast(batchId)
      val refDataBcast = sc.broadcast(refData)

      //FailSafeInputFormat.setInputFormat[TextInputFormat](sc.hadoopConfiguration)
      inputDataType.setConfig(sc.hadoopConfiguration)

      //val hdd = sc.newAPIHadoopFile(paths, classTag[I].runtimeClass.asInstanceOf[Class[I]], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]

      val hdd = inputDataType match {
        case rp:TextInputDataFormat => {
          if (encryptionInfo.enabled) {
            EncryptedTextInputFormat.setPrivateKeyFileName(sc.hadoopConfiguration, encryptionInfo.privateKeyFile)
          }
          sc.newAPIHadoopFile(paths, classOf[FailSafeInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]
        }
        case rp:XMLInputDataFormat => sc.newAPIHadoopFile(paths, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]
        case rp:BadXMLInputDataFormat => sc.newAPIHadoopFile(paths, classOf[BadXmlInputFormat], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      }

      //val hdd = sc.newAPIHadoopFile(paths, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text]).asInstanceOf[NewHadoopRDD[LongWritable, Text]]

      hdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
        val file = inputSplit.asInstanceOf[org.apache.hadoop.mapreduce.lib.input.FileSplit]
        def textOrSplitName(text: Text) = if (text == null) file.getPath.toString else text.toString
        iterator.map {
          case (null, text) => Left(ProcessingFailure(new Exception("BAD-FILE"), Right(LineRecord(file.getPath.getName, textOrSplitName(text), batchIdBcast.value, refDataBcast.value))))
          case (_, text) ⇒ Right(LineRecord(file.getPath.getName, text.toString, batchIdBcast.value, refDataBcast.value))
        }
      }
    }
  }

  implicit class EitherTrappableRDD[T : ClassTag](rdd: RDD[Either[ProcessingFailure, T]]) {

    def trapToHdfs(trap: Trap, prefix: String): RDD[T] = trap.trap(rdd, prefix)
  }

  implicit class PartialRDD(rdd:RDD[LineRecord]) {

    def parseData[P](fn:LineRecord => Either[ProcessingFailure,PartialRecord[P]]):RDD[Either[ProcessingFailure, PartialRecord[P]]] = {
      rdd.map(fn)
    }
  }

  implicit class EnrichableRDD[X <: SpecificRecord : ClassTag](rdd: RDD[Either[ProcessingFailure, PartialRecord[X]]]) {

    def enrichWith(fn: Either[ProcessingFailure, PartialRecord[X]] => Either[ProcessingFailure, RichRecord[X]], interval: Interval): RDD[Either[ProcessingFailure, RichRecord[X]]] = {
      val intervalStartMillis = interval.getStartMillis
      val intervalEndMillis = interval.getEndMillis

      rdd.map(fn)
        .map(_ match {
          case Left(error) => Left(error)
          case Right(richRecord) =>
            val eventMillis = richRecord.eventTime.getMillis
            if (eventMillis >= intervalStartMillis && eventMillis < intervalEndMillis) {
              Right(richRecord)
            }
            else {
              Left(ProcessingFailure(new OutOfRangeException(eventMillis, intervalStartMillis, intervalEndMillis), Right(richRecord)))
            }
        })
    }
  }

  implicit class DateTimeSortableRDD[T](rdd: RDD[RichRecord[T]]) extends Utility{
    def shuffleRandomlyAndSortByEventTime(parallelism: Int, seed: Int = System.currentTimeMillis.toInt): RDD[RichRecord[T]] = {
      rdd
        .map(_.transformedForPartition)
        .mapPartitions { part =>
          val r = new Random(seed)
          part.map { x => ((x._1, r.nextInt()), x._2)}
        }
        .repartitionAndSortWithinPartitions(new HashPartitioner(parallelism))
        .values
    }
  }

  implicit class TransformableForWriteRDD[T](rdd: RDD[RichRecord[T]]) {
    def transformedForWrite(): RDD[(LocalDate, T)] = {
      rdd.map(_.transformedForWrite)
    }
  }

  implicit class SecuredOutputRDD[R <: SpecificRecord : ClassTag](val rdd: RDD[(LocalDate, R)]) {
    def saveOutput(output: JobOutput): List[Path] = {
      output.saveToSink(rdd)
    }
  }
}