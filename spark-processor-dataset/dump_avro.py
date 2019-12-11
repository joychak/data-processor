import sys
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open(sys.argv[1]).read())

reader = DataFileReader(open(sys.argv[2], "r"), DatumReader())
for g in reader:
    print g
reader.close()
