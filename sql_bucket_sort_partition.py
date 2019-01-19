from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql load save").getOrCreate()

df = spark.read.load("C:\\Users\lenovo\Documents\\tmp\data.json", format("json"))
df.write.bucketBy(3, "name").sortBy("age").saveAsTable("people_bucket")
sqldf = spark.sql("select * from people_bucket")
sqldf.show()

df.write.partitionBy("age").format("parquet").save("C:\\Users\lenovo\Documents\\tmp\\namesbyage.parquet")
