package com.datalogs.inputformats

import java.util

import com.datalogs.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util.ReflectionUtils

import scala.reflect._
import scala.util.control.NonFatal

class FailSafeInputFormat[K, V] extends InputFormat[K, V] with Logging {

  private def baseInputFormat(job: Configuration): InputFormat[K, V] = {
    FailSafeInputFormat.getInputFormat[K, V](job)
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[K, V] = {
    val in = baseInputFormat(context.getConfiguration)

    new RecordReader[K, V] {
      var fileBroken: Boolean = false
      var fileBrokenAtInitialize: Boolean = false
      var key: K = null.asInstanceOf[K]
      var value: V = null.asInstanceOf[V]

      val rr = in.createRecordReader(split, context)

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
        try {
          rr.initialize(split, context)
        } catch {
          case NonFatal(e) => {
            fileBrokenAtInitialize = true
            logError(s"Failed to initialize!  Something wrong with the file? ${split} / ${rr}")
          }
        }
      }

      override def close(): Unit = rr.close()
      override def getProgress: Float = rr.getProgress
      override def nextKeyValue(): Boolean = {
        if (fileBroken) {
          key = null.asInstanceOf[K]
          value = null.asInstanceOf[V]
          return false
        }
        if (fileBrokenAtInitialize) {
          fileBroken = true
          key = null.asInstanceOf[K]
          value = null.asInstanceOf[V]
          return true
        }
        try {
          rr.nextKeyValue()
        } catch {
          case NonFatal(e) => {
            fileBroken = true
            logError(s"Failed to read next key, value!  Something wrong with the file? ${split} / ${rr}")
          }
        }

        if (fileBroken) {
          key = null.asInstanceOf[K]
          value = null.asInstanceOf[V]
          true
        }
        else {
          key = rr.getCurrentKey
          value = rr.getCurrentValue
          key match {
            case null => false
            case _ => true
          }
        }
      }

      override def getCurrentValue: V = value
      override def getCurrentKey: K = key
    }
  }

  override def getSplits(context: JobContext): util.List[InputSplit] = {
    val in = baseInputFormat(context.getConfiguration)
    in.getSplits(context)
  }
}

object FailSafeInputFormat {
  final val InputFormatClass = "datalogs.failsafe.inputformat.class"

  def getInputFormat[K, V](conf: Configuration): InputFormat[K, V] = {
    val clz = conf.getClass(InputFormatClass, classOf[InputFormat[K, V]], classOf[InputFormat[K, V]])
    ReflectionUtils.newInstance(clz, conf)
  }

  def setInputFormat[I <: InputFormat[_, _] : ClassTag](conf: Configuration): Unit = {
    conf.setClass(InputFormatClass, classTag[I].runtimeClass, classTag[InputFormat[_, _]].runtimeClass)
  }
}

