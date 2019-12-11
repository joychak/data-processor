package com.datalogs.outputformats

import com.datalogs.Utility
import org.apache.avro.mapred.{AvroOutputFormat, AvroWrapper}
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.{Progressable, ReflectionUtils}
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.language.higherKinds
import scala.reflect.{ClassTag, classTag}

trait Partitioner[K] {
  def assignPartition(value: K): String
}

trait PartitionerFactory[K] {
  def getPartitioner(conf: Configuration): Partitioner[K]
}
object PartitionerFactory {
  final val PartitionerFactoryClass = "datalogs.partitioner.factory.class"

  def setPartitionerFactory[PF <: PartitionerFactory[_] : ClassTag](conf: Configuration) = {
    conf.setClass(PartitionerFactoryClass, classTag[PF].runtimeClass, classTag[PF].runtimeClass)
  }

  def getPartitionerFactory[K](conf: Configuration): PartitionerFactory[K] = {
    val clz = conf.getClass(PartitionerFactoryClass, classOf[PartitionerFactory[K]], classOf[PartitionerFactory[K]])
    ReflectionUtils.newInstance(clz, conf)
  }
}

class DateAndBatchPartitionerFactory extends PartitionerFactory[LocalDate] with Utility {
  override def getPartitioner(conf: Configuration): Partitioner[LocalDate] =
    new Partitioner[LocalDate] {
      val batchId = DateAndBatchPartitionerFactory.getBatchId(conf)
      val eventDateField = DateAndBatchPartitionerFactory.getEventDateField(conf)
      val batchIdField = DateAndBatchPartitionerFactory.getBatchIdField(conf)

      override def assignPartition(value: LocalDate): String = {
        DateAndBatchPartitionerFactory.assignPartition(value, batchId, eventDateField, batchIdField) / "part"
      }
    }
}
object DateAndBatchPartitionerFactory  extends Utility{
  final val BatchId = "datalogs.batch.id"
  final val EventDateField = "datalogs.eventDate.field"
  final val BatchIdField = "datalogs.batchId.field"

  final val DefaultEventDateField = "eventDate"
  final val DefaultBatchIdField = "batchId"

  private val df = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC() // this is thread safe

  def setPartitionerFactory(conf: Configuration, batchId: String, eventDateField: String = DefaultEventDateField, batchIdField: String = DefaultBatchIdField): Unit = {
    PartitionerFactory.setPartitionerFactory[DateAndBatchPartitionerFactory](conf)
    conf.set(BatchId, batchId)
    conf.set(EventDateField, eventDateField)
    conf.set(BatchIdField, batchIdField)
  }

  def getBatchId(conf: Configuration): String = {
    conf.get(BatchId)
  }

  def getEventDateField(conf: Configuration): String = {
    conf.get(EventDateField)
  }

  def getBatchIdField(conf: Configuration): String = {
    conf.get(BatchIdField)
  }

  def assignPartition(value: LocalDate, batchId: String, eventDateField: String = DefaultEventDateField, batchIdField: String = DefaultBatchIdField): String = {
    "%s=%s".format(eventDateField, df.print(value)) / "%s=%s".format(batchIdField, batchId)
  }
}

trait PartitionRecordWriterFactory[K, V] {
  def getRecordWriter(fs: FileSystem, job: JobConf, name: String, reporter: Progressable): RecordWriter[K, V]
}
object PartitionedRecordWriterFactory {
  final val RecordWriterFactory = "datalogs.partitioned.recordwriterfactory"

  def setRecordWriterFactory[RWF <: PartitionRecordWriterFactory[_, _] : ClassTag](conf: Configuration): Unit = {
    conf.setClass(RecordWriterFactory, classTag[RWF].runtimeClass, classTag[RWF].runtimeClass)
  }

  def getRecordWriterFactory[K, V](conf: Configuration): PartitionRecordWriterFactory[K, V] = {
    val clz = conf.getClass(RecordWriterFactory, classOf[PartitionRecordWriterFactory[K, V]], classOf[PartitionRecordWriterFactory[K, V]])
    ReflectionUtils.newInstance(clz, conf)
  }
}

class ParquetRecordWriterFactory[K, V <: SpecificRecord] extends PartitionRecordWriterFactory[K, V] {
  override def getRecordWriter(fs: FileSystem, job: JobConf, name: String, reporter: Progressable): RecordWriter[K, V] =
    new RecordWriter[K, V] {
      val of = new DeprecatedParquetOutputFormat[V]
      val rr = of.getRecordWriter(fs, job, name, reporter)

      override def write(key: K, value: V): Unit = rr.write(null, value)
      override def close(reporter: Reporter): Unit = rr.close(reporter)
    }
}
class AvroSpecificRecordRecordWriterFactory[K, V <: SpecificRecord] extends PartitionRecordWriterFactory[K, V] {
  override def getRecordWriter(fs: FileSystem, job: JobConf, name: String, reporter: Progressable): RecordWriter[K, V] =
    new RecordWriter[K, V] {
      val of = new AvroOutputFormat[V]
      val rr = of.getRecordWriter(fs, job, name, reporter)

      override def write(key: K, value: V): Unit = rr.write(new AvroWrapper[V](value), null)
      override def close(reporter: Reporter): Unit = rr.close(reporter)
    }
}

