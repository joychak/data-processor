package com.datalogs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

/**
  * Created by user-1 on 5/17/17.
  */
trait DataLogsSparkSession {

  implicit private[this] class SparkSessionBuilderOperations(builder: Builder) {
    def setMaster(master: String) = if (master.length > 0) builder.master(master) else builder

    def setAppName(appName: String) = if (appName.length > 0) builder.appName(appName) else builder

    def setConfig(key: String, value: String) = builder.config(key, value)

    def setEnableHiveSupport(enabled: Boolean) = if (enabled) builder.enableHiveSupport() else builder

    def setConfigs(configs: Map[String, String]) = {
      configs.foldLeft(builder)((builder: Builder, configElement) => builder.config(configElement._1, configElement._2))
    }
  }

  protected[this] def prepare(master: String,
                              appName: String,
                              configs: Map[String, String],
                              enableHiveSupport: Boolean): SparkSession = {
    SparkSession.builder()
      .setMaster(master)
      .setAppName(appName)
      .setConfigs(configs)
      .setEnableHiveSupport(enableHiveSupport)
      .getOrCreate()
  }
}

object DataLogsSparkSession extends DataLogsSparkSession {

  def getOrCreate(master: String = "",
                  appName: String = "",
                  configs: Map[String, String] = Map.empty[String, String],
                  enableHiveSupport: Boolean = false): SparkSession = {
    prepare(master, appName, configs, enableHiveSupport)
  }
}
