//package com.datalogs.sparkmetrics
//
//import java.io.Serializable
//import org.joda.time.DateTime
//import scala.collection.concurrent.TrieMap
//
//case class StageToTask(stageId:Int, taskId:Long)
//case class RecordsProcessedPerStage(stageId:Int, recordProcessed:Long)
////case class Metrics(categoryName:String = "Default", metricName:String = "Default", applicationRunTime:Long = 0 , RecordsProcessed:Long = 0, RecordsWritten:Long = 0, BadRecords:Long = 0)
//
//
//object SparkJobMetrics extends Serializable {
//
//  private[this]  var _jobStartTime: Long = new DateTime().getMillis
//  def resetJobTime : Unit = {
//    _jobStartTime = new DateTime().getMillis
//  }
//  def getJobRunTimeInSeconds = (DateTime.now().getMillis - _jobStartTime) / 1000
//
//  private[this] var _stageInfoMap: TrieMap[Int, String] = new TrieMap[Int,String]
//  def stageInfoMap = _stageInfoMap
//
//  private[this] var recordsRead: TrieMap[StageToTask, Long] = new TrieMap[StageToTask,Long]
//  def addRecordsRead(stageId : Int, taskId : Long, count : Long) = recordsRead.put(StageToTask(stageId, taskId), count)
//  def totRecordsRead : Long = {
//    recordsRead.isEmpty match {
//      case true => 0L
//      case false =>
//        val sumByStageIdMap = sumMapCountersByStageId(recordsRead)
//        sumByStageIdMap.valuesIterator.max
//    }
//  }
//
//  private[this] var recordsWritten: TrieMap[StageToTask, Long] = new TrieMap[StageToTask,Long]
//  def addRecordsWritten(stageId : Int, taskId : Long, count : Long) = recordsWritten.put(StageToTask(stageId, taskId), count)
//  def totRecordsWritten : Long = {
//    recordsWritten.isEmpty match {
//      case true => 0L
//      case false =>
//        val sumByStageIdMap = sumMapCountersByStageId(recordsWritten)
//        sumByStageIdMap.getOrElse(sumByStageIdMap.keys.max, 0L)
//    }
//  }
//
//  private def sumMapCountersByStageId(map : TrieMap[StageToTask, Long]) = map.groupBy(_._1.stageId).mapValues(_.map(_._2).sum)
//
//  def resetJobCounters : Unit = {
//    stageInfoMap.clear()
//    recordsRead.clear()
//    recordsWritten.clear()
//  }
//}