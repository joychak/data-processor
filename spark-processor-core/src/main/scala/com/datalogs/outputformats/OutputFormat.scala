package com.datalogs.outputformats

import com.datalogs.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.MultipleOutputFormat
import org.apache.hadoop.util.Progressable

class PartitionedOutputFormat[K, V] extends MultipleOutputFormat[K, V] with Logging {
  var partitioner: Partitioner[K]            = _
  var rw: PartitionRecordWriterFactory[K, V] = _

  // An exception is thrown since the outputDir already exists... but it will ALWAYS exist!
  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    try {
      super.checkOutputSpecs(ignored, job)
    } catch {
      case e: FileAlreadyExistsException => //
    }
  }

  override def getRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[K, V] = {
    job.getOutputCommitter match {
      case c: FileOutputCommitter => {
        val tid = TaskAttemptID.forName(job.get("mapred.task.id"))
        val context = new TaskAttemptContextImpl(job, tid)
        val p = c.getTaskAttemptPath(context)
        logInfo(s"Detected FileOutputCommitter... Calling FileOutputFormat.setWorkOutputPath to `${p}`")
        FileOutputFormat.setWorkOutputPath(job, c.getTaskAttemptPath(context))
      }
      case _ => // do nothing
    }

    val pf = PartitionerFactory.getPartitionerFactory[K](job)
    partitioner = pf.getPartitioner(job)
    rw          = PartitionedRecordWriterFactory.getRecordWriterFactory[K, V](job)
    super.getRecordWriter(fs, job, name, arg3)
  }

  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = partitioner.assignPartition(key)

  override def getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[K, V] = {
    rw.getRecordWriter(fs, job, name, arg3)
  }
}
