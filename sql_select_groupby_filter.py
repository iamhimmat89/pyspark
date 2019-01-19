from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_ops").getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.select("name").show()
df.filter(df['age'] > 25).show()
df.groupBy("age").count().show()
