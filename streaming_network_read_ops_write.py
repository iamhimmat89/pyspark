from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.\
    master("local").\
    appName("Streaming_Create_DF").\
    getOrCreate()

lines = spark.readStream.\
    format("socket").\
    option("host", "localhost").\
    option("port", "9999").\
    load()

query = lines.writeStream.format("csv"). \
    option("path", "C:\\Users\lenovo\Documents\\tmp\stream\in"). \
    option("checkpointLocation", "C:\\Users\lenovo\Documents\\tmp\stream\ck"). \
    start()

peopleSchema = StructType().add("name", "string").add("age", "integer")
peopleDF = spark.readStream.option("sep", ";").schema(peopleSchema).csv("C:\\Users\lenovo\Documents\\tmp\stream\in")

peopleDF.createOrReplaceTempView("people")
result = spark.sql("select name, age from people")
result.writeStream.format("json").outputMode('append').\
    option("path", "C:\\Users\lenovo\Documents\\tmp\stream\out").\
    option("checkpointLocation", "C:\\Users\lenovo\Documents\\tmp\stream\outck").\
    start()

query.awaitTermination()
