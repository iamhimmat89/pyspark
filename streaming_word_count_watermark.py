from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, window

spark = SparkSession.builder.\
    master("local").\
    appName("Streaming_Word_Counts_Watermark").\
    getOrCreate()

lines = spark.readStream.\
    format("socket").\
    option("host", "localhost").\
    option("port", "9999").\
    option('includeTimestamp', 'true').\
    load()

words = lines.select(explode(split(lines.value, " ")).alias("word"), lines.timestamp)
wordCounts = words.withWatermark("timestamp", "1 minutes").groupBy(
    window(words.timestamp, "1 minutes", "30 seconds"),
    words.word
    ).count()

output = wordCounts.writeStream.outputMode("complete").format("console").start()
output.awaitTermination()
