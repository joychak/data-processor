package com.datalogs.common

import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase

class DatalogsNamingStrategy(colMap:Map[String,String]) extends PropertyNamingStrategyBase {

  private def isEmpty(input:String) = Option(input).forall(_.isEmpty)

  @Override
  def translate (input:String):String = {
    input match {
      case emptyStr if isEmpty(input) => input
      case _ => colMap.getOrElse(input, input)
    }
  }
}
