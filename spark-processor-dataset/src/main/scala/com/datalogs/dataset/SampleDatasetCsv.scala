package com.datalogs.dataset

import com.datalogs.{AvroSchemaRegistrator, BaseProcessor, DateTimeContext, LineRecord, PartialRecord, RichRecord}
import com.datalogs.common.{DatalogsEnricher, DatalogsFilter, DatalogsMetaInfo, DatalogsParser}
import com.datalogs.dataschema.{MetaInfo, SampleDatasetCsv}
import com.esotericsoftware.kryo.Kryo
import org.apache.avro.AvroTypeException
import org.apache.avro.specific.SpecificData
import org.joda.time.{DateTime, DateTimeZone}

object SampleDatasetCsvProcessor extends BaseProcessor[SampleDatasetCsv](classOf[SampleDatasetCsvAvroSchemaRegistrator],
  SampleDatasetCsvParser, SampleDatasetCsvEnricher, DatalogsFilter)

//SampleDatasetCsv Parser with Data: "utc_datetime":"2018-04-25T21:58:21.000","id":"ABC1234","field1":10723484,"field2":1,"field3":0,"field4":0,"field5":0,"field6":208,"field7":""
object SampleDatasetCsvParser extends DatalogsParser[SampleDatasetCsv] {

  override def getHostName(fileName: String):String = "TEST"

  override def getRecordObject(recordObj: LineRecord): SampleDatasetCsv = {

    val tokens = recordObj.line.replace("\"","").trim().split(",")
    val builder = SampleDatasetCsv.newBuilder()

    if (tokens.length == 6) {
      builder.
        setId(tokens(0).toInt).
        setField1(tokens(1)).
        setField2(tokens(2)).
        setField3(tokens(3).toInt).
        setField4(tokens(4)).
        setUtcDatetime(tokens(5)).
        setMetaInfo(new MetaInfo)
    }

    builder.build()
  }
}

//SampleDatasetJson enricher to add metadata
object SampleDatasetCsvEnricher extends DatalogsEnricher[SampleDatasetCsv] {

  //Get UTC time in milli-second
  override def getUTCMillis(partialData:PartialRecord[SampleDatasetCsv]): Long = {
    df.parseDateTime(partialData.getRecord.getUtcDatetime.toString).getMillis
  }

  override def getDTC(partialData:PartialRecord[SampleDatasetCsv]): DateTimeContext = {
    DateTimeContext(new DateTime(partialData.getRecord.getMetaInfo.getEventUTC, DateTimeZone.UTC))
  }

  override def enrichWithMetadata(partialData:PartialRecord[SampleDatasetCsv]):RichRecord[SampleDatasetCsv] = {
    partialData.getRecord.setMetaInfo(DatalogsMetaInfo(
      getUTCMillis(partialData), getSourceTZone(partialData), partialData.getHostName, partialData.getSourceName, partialData.getBatchId ))

    val b = SampleDatasetCsv.newBuilder(partialData.getRecord).build()
    if (!SpecificData.get().validate(SampleDatasetCsv.getClassSchema, b)) throw new AvroTypeException("Error in build")

    RichRecord(b, getDTC(partialData))
  }
}

//register the SampleDatasetJson Kyro for avro schema
class SampleDatasetCsvAvroSchemaRegistrator extends AvroSchemaRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SampleDatasetCsv])
    super.registerClasses(kryo)
  }
}