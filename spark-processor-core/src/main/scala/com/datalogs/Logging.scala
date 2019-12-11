package com.datalogs

import org.slf4j.{Logger, LoggerFactory}
/**
  * Created by user-1 on 5/16/17.
  */
trait Logging {
  private var logger_ : Logger = null

  def loggerName : String = this.getClass.getName

  private def logger : Logger = {
    if (logger_ == null) {
      logger_ = LoggerFactory.getLogger(loggerName)
    }
    logger_
  }
  def isDebugEnabled = logger.isDebugEnabled

  def logTrace(msg: => String, throwable: Throwable = null) = logger.trace(msg)
  def logDebug(msg: => String, throwable: Throwable = null) = logger.debug(msg)
  def logInfo(msg: => String, throwable: Throwable = null) = logger.info(msg)
  def logWarning(msg: => String, throwable: Throwable = null) = logger.warn(msg)
  def logError(msg: => String, throwable: Throwable = null) = logger.error(msg, throwable)
}