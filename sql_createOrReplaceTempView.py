from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_temp_view").getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.createOrReplaceTempView("people")
sqldf = spark.sql("select * from people")
sqldf.show()
