from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_global_temp_view").getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.createGlobalTempView("g_people")
spark.sql("select * from global_temp.g_people").show()
