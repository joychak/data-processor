//package com.datalogs.sparkmetrics
//
//
//import com.datalogs.Logging
//import org.apache.spark.scheduler._
//
////import com.datalogs..{SparkJobMetrics, StageToTask}
//
//class SparkJobMetricsCollector extends SparkListener with Logging {
//
//  logInfo("RERUNLOG SparkJobMetricsCollector instance created")
//
//  override def onStageCompleted(stageCompleted:SparkListenerStageCompleted):Unit = {
//    SparkJobMetrics.stageInfoMap.put(stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.name)
//  }
//
//  override def onTaskEnd(taskEnd: SparkListenerTaskEnd):Unit= {
//
//    taskEnd.taskInfo.successful match {
//      case true => {
//        // Record Input Metrics
//        (taskEnd.taskMetrics.inputMetrics != null) match {
//          case true => {
//            SparkJobMetrics.addRecordsRead(taskEnd.stageId, taskEnd.taskInfo.taskId, taskEnd.taskMetrics.inputMetrics.recordsRead)
//          }
//          case false => //logInfo ("ListenerMetrics task running and still active")
//        }
//        //Record Ouput Metrics
//        (taskEnd.taskMetrics.outputMetrics != null) match {
//          case true => {
//            SparkJobMetrics.addRecordsWritten(taskEnd.stageId, taskEnd.taskInfo.taskId, taskEnd.taskMetrics.outputMetrics.recordsWritten)
//          }
//          case false => //Do Nothing
//        }
//      }
//      case _ => //logInfo("ListenerMetrics task failed " + taskEnd.taskInfo.id)
//    }
//  }
//}
//
//
//
