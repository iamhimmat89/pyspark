from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession.builder.\
    master("local").\
    appName("Streaming_Word_Counts").\
    getOrCreate()

lines = spark.readStream.\
    format("socket").\
    option("host", "localhost").\
    option("port", "9999").\
    load()

words = lines.select(explode(split(lines.value, " ")).alias("word"))
wordCounts = words.groupBy(words.word).count()

query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
