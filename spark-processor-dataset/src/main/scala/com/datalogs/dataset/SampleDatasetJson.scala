package com.datalogs.dataset

import com.datalogs.{AvroSchemaRegistrator, BaseProcessor, DateTimeContext, LineRecord, PartialRecord, RichRecord}
import com.datalogs.common.{DatalogsEnricher, DatalogsFilter, DatalogsMetaInfo, DatalogsParser}
import com.datalogs.dataschema.{MetaInfo, SampleDatasetJson}
import com.esotericsoftware.kryo.Kryo
import org.apache.avro.AvroTypeException
import org.apache.avro.specific.SpecificData
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.control.NonFatal

object SampleDatasetJsonProcessor extends BaseProcessor[SampleDatasetJson](classOf[SampleDatasetJsonAvroSchemaRegistrator],
  SampleDatasetJsonParser, SampleDatasetJsonEnricher, DatalogsFilter)

//SampleDatasetJson Parser with Data: "utc_datetime":"2018-04-25T21:58:21.000","id":"ABC1234","field1":10723484,"field2":1,"field3":0,"field4":0,"field5":0,"field6":208,"field7":""
object SampleDatasetJsonParser extends DatalogsParser[SampleDatasetJson] {
  //Get the host name from SampleDatasetJson file name
  override def getHostName(fileName: String):String = {
    try {
      fileName.split("_")(5)
    } catch {
      case NonFatal(e) => "N/A"
    }
  }

  //Get the SampleDatasetJson record object from Line record
  override def getRecordObject(recordObj: LineRecord): SampleDatasetJson = {
    val b = mapper.readValue(getRecordString(recordObj), classOf[SampleDatasetJson.Builder])
    b.setMetaInfo(new MetaInfo)
    b.build()
  }
}

//SampleDatasetJson enricher to add metadata
object SampleDatasetJsonEnricher extends DatalogsEnricher[SampleDatasetJson] {

  //Get UTC time in milli-second
  override def getUTCMillis(partialData:PartialRecord[SampleDatasetJson]): Long = {
    df.parseDateTime(partialData.getRecord.getUtcDatetime.toString).getMillis
  }

  override def getDTC(partialData:PartialRecord[SampleDatasetJson]): DateTimeContext = {
    DateTimeContext(new DateTime(partialData.getRecord.getMetaInfo.getEventUTC, DateTimeZone.UTC))
  }

  override def enrichWithMetadata(partialData:PartialRecord[SampleDatasetJson]):RichRecord[SampleDatasetJson] = {
    partialData.getRecord.setMetaInfo(DatalogsMetaInfo(
      getUTCMillis(partialData), getSourceTZone(partialData), partialData.getHostName, partialData.getSourceName, partialData.getBatchId ))

    val b = SampleDatasetJson.newBuilder(partialData.getRecord).build()
    if (!SpecificData.get().validate(SampleDatasetJson.getClassSchema, b)) throw new AvroTypeException("Error in build")

    RichRecord(b, getDTC(partialData))
  }
}

//register the SampleDatasetJson Kyro for avro schema
class SampleDatasetJsonAvroSchemaRegistrator extends AvroSchemaRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SampleDatasetJson])
    super.registerClasses(kryo)
  }
}