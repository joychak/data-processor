package com.datalogs.resources.statestore

import java.io.PrintWriter

import com.datalogs.Logging
import com.datalogs.resources.{JobBatch, JobSubmitConf}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.joda.time.DateTime

/**
  * Interface for Reprocess State Store
  */
trait BatchReprocessStateStore {
  def markBatchSuccess(successBatchId: String) : Unit
  def markBatchFailure(failedBatchId: String) : Unit
  def getNextBatchId() : Option[String]
  def flush() : Unit
}

class BatchReprocessState (sc: SparkContext, datasource: String, reprocessDirectory: Path, scheduledEndTime: DateTime) extends BatchReprocessStateStore with Logging {

  // Build an inventory of batches to be reprocessed
  // Start with empty inventory
  var inventory = scala.collection.mutable.Map.empty[String, String]
  private val fs = reprocessDirectory.getFileSystem(sc.hadoopConfiguration)

  private val allFailedBatchesFile = new Path(reprocessDirectory.toString + "/" + datasource + "-failed-batches.txt")
  private var allFailedInventory = initInventory(allFailedBatchesFile, scala.collection.mutable.Map.empty[String, String])

  private val reprocessStateStoreCreationTimestamp = System.currentTimeMillis()
  private val consolidatedReprocessFile = new Path(reprocessDirectory.toString + "/" + datasource + "-to-reprocess-" + reprocessStateStoreCreationTimestamp + ".system.txt")

  private def initInventory(inventorySource: Path, acc: scala.collection.mutable.Map[String, String]): scala.collection.mutable.Map[String, String] = {

    logInfo(s"RERUNLOG Building Inventory from ${inventorySource.toString}")
    (fs.exists(inventorySource) && fs.isFile(inventorySource)) match {
      case false => acc
      case true => {
        scala.io.Source.fromInputStream(fs.open(inventorySource))
          .getLines()
          .foldLeft(acc) {
            (_, line) => {
              line.split("""\|""") match {
                case Array(reprocessBatchId, reprocessBatchArgs) =>
                  acc += (reprocessBatchId -> reprocessBatchArgs)
                case _ => logWarning(s"Could not split line ${line}")
                  acc
              }
            }
          }
      }
    }
  }

  private def writeMapToFile(map: scala.collection.mutable.Map[String, String], destfile: Path, delimiter: String = "|"): Unit = {
    logInfo(s"RERUNLOG ReprocessStateStore.writeMapToFile ${destfile.toString} <- ${map.size}")

    // This will create empty file for
    val pw = new PrintWriter(fs.create(destfile, true))
    map.foreach(kv => pw.println(s"${kv._1}${delimiter}${kv._2}${delimiter}"))
    pw.close()
  }

  private def initReprocessInventory(): scala.collection.mutable.Map[String, String] = {

    // File(s) that contain list of batches to be reprocessed
    val toReprocessBatchesFiles = {
      //+ "/" + datasource + "-to-reprocess-*.txt")
      (fs.exists(reprocessDirectory) && fs.isDirectory(reprocessDirectory)) match {
        case false => List[Path]()
        case true =>
          fs.listStatus(reprocessDirectory)
            .filter(fileStatus => fileStatus.getPath.toString.contains(s"${datasource}-to-reprocess"))
            .map(fileStatus => fileStatus.getPath)
            .toList

      }
    }

    logInfo(s"RERUNLOG ToReprocessBatchesFiles = ${toReprocessBatchesFiles.mkString(" ")}")

    if (toReprocessBatchesFiles.size > 0) {

      // Accumulate into inventory, contents of each file
      toReprocessBatchesFiles.foreach(p => {
        inventory = initInventory(p, inventory)
      })

      // Write a consolidated file containing all batches that must be reprocesed
      writeMapToFile(inventory, consolidatedReprocessFile)

      // Get rid of all the (small) file(s)
      toReprocessBatchesFiles.foreach(p => fs.delete(p, false))
    }
    // Return the inventory
    inventory
  }

  private def timeForMoreBatches() : Boolean = {
    val rightNowMillis = System.currentTimeMillis()
    logInfo(s"ScheduledEndTime = ${scheduledEndTime.getMillis}, RightNow = ${rightNowMillis}")
    scheduledEndTime.getMillis > rightNowMillis
  }

  private var toReprocessInventory = initReprocessInventory()

  // Implementaion of public interface
  def markBatchSuccess(successBatchId: String) : Unit = {
    allFailedInventory -= successBatchId
  }

  def markBatchFailure(failedBatchId: String) : Unit = {
    allFailedInventory += (failedBatchId -> "*")
  }

  def getNextBatchId() : Option[String] = {
    if (timeForMoreBatches()) {
      logInfo("RERUNLOG There is still time to run another reprocess batch")
      toReprocessInventory.isEmpty match {
        case true =>
          logInfo("RERUNLOG However, there aren't any more batches pending to be reprocessed")
          None
        case false =>
          val head = toReprocessInventory.head
          toReprocessInventory -= head._1
          logInfo(s"RERUNLOG Next reprocess batch is ${head._1}")
          Some(head._1)
      }
    }
    else {
      logInfo("RERUNLOG Its too late to be running anymore reprocesses")
      None
    }
  }

  def print() : Unit = {
    logInfo("Failed Batches ...")
    allFailedInventory.keys.foreach(logInfo(_))

    logInfo("ToReprocess Batches ...")
    toReprocessInventory.keys.foreach(logInfo(_))
  }

  def flush() : Unit = {
    // write back the files
    logInfo("RERUNLOG Flushing ReprocessStateStore")
    writeMapToFile(allFailedInventory, allFailedBatchesFile)

    toReprocessInventory.size match {
      case 0 =>
        logInfo(s"RERUNLOG Deleting ${consolidatedReprocessFile.toString}")
        fs.delete(consolidatedReprocessFile, false)
      case _ => writeMapToFile(toReprocessInventory, consolidatedReprocessFile)
    }
  }
}

object BatchReprocessState extends Logging {

  private def computeScheduledEndTime(opts: JobSubmitConf) : DateTime = JobBatch.getInterval(opts.batchId(), opts.duration()).getEnd.plusMinutes(opts.duration())

  def prepare(sc: SparkContext, opts: JobSubmitConf, computeScheduledEndTimeFunc : (JobSubmitConf => DateTime) = computeScheduledEndTime): BatchReprocessState = {
    logInfo("RERUNLOG Creating ReprocessStateStore")
    // The scheduledEndTime here is the time till which this batch can continue to run (and process previously failed
    // batches) before its time for next timed batch
    val scheduledEndTime = computeScheduledEndTimeFunc(opts)
    val reprocessStateStore = new BatchReprocessState(sc, opts.dataSourceName().toLowerCase, opts.reprocessStoreDir, scheduledEndTime)
    reprocessStateStore.print()
    logInfo("RERUNLOG Created ReprocessStateStore")

    reprocessStateStore
  }
}

