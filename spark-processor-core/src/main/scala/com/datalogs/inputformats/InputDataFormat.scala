package com.datalogs.inputformats

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import scala.reflect._

abstract class InputDataFormat[+I <: InputFormat[LongWritable, Text] : ClassTag] {
  def setConfig(conf: Configuration): Unit
}

case class TextInputDataFormat(inputEncrypted : Boolean = false) extends InputDataFormat[FailSafeInputFormat[LongWritable, Text]] {
  override def setConfig(conf: Configuration): Unit = {
    inputEncrypted match {
      case true => FailSafeInputFormat.setInputFormat[EncryptedTextInputFormat[LongWritable, Text]](conf)
      case false => FailSafeInputFormat.setInputFormat[TextInputFormat](conf)
    }
  }
}

private[inputformats] abstract class XMLData[I <: InputFormat[LongWritable, Text] : ClassTag](xmlTag: String) extends InputDataFormat[I] {
  override def setConfig(conf: Configuration): Unit = {
    conf.set(XmlInputFormat.START_TAG_KEY, s"<${xmlTag}>")
    conf.set(XmlInputFormat.END_TAG_KEY, s"</${xmlTag}>")
    conf.set(XmlInputFormat.ENCODING_KEY, "utf-8")
  }
}
case class XMLInputDataFormat(xmlTag: String) extends XMLData[XmlInputFormat](xmlTag)
case class BadXMLInputDataFormat(xmlTag: String) extends XMLData[BadXmlInputFormat](xmlTag)






