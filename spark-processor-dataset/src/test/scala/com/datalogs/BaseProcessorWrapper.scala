package com.datalogs

import com.datalogs.resources.JobSubmitConf
import org.apache.spark.serializer.KryoSerializer

import scala.collection.immutable.HashMap

object BaseProcessorWrapper {

  def run(appName : String = "TestApp",
          appArgs : Array[String] = Array.empty[String],
          processor: BaseProcessor[_],
          appConfigs : HashMap[String, String] = HashMap.empty[String, String]) = {

    val jobConf = new JobSubmitConf(appArgs)
    val batchId = jobConf.batchId()

    val configs = appConfigs ++ HashMap[String, String](
      "spark.serializer" -> classOf[KryoSerializer].getName,
      "spark.kryo.registrator" -> processor.register.getName,
      "spark.driver.allowMultipleContexts" -> "true"
    )

    val sparkSession = DataLogsSparkSession.getOrCreate(
      master = "local",
      appName = appName,
      configs = appConfigs,
      enableHiveSupport = false
    )
    processor.runBatches(sparkSession, jobConf, batchId)
  }
}
