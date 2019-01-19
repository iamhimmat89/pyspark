from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_fillna").getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.show()
df.fillna('unknown').show()
