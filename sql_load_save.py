from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql load save").getOrCreate()

df = spark.read.load("C:\\Users\lenovo\Documents\\tmp\data.json", format("json"))
df.select("name", "age").write.save("C:\\Users\lenovo\Documents\\tmp\data.parquet", format("parquet"))

df_1 = spark.read.load("C:\\Users\lenovo\Documents\\tmp\data.csv", format="csv", sep=":", inferSchema="true", header="true")
df_1.show()

df_2 = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df_2.show()
