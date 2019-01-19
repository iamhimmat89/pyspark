from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql df json").getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.show()
df.printSchema()
df.select("name").show()
df.select(df['name'], df['age'] + 1).show()
df.filter(df['age'] > 25).show()
df.groupBy("age").count().show()

df.createOrReplaceTempView("people")
sqldf = spark.sql("select * from people")
sqldf.show()

df.createGlobalTempView("g_people")
spark.sql("select * from global_temp.g_people").show()
spark.newSession().sql("select * from global_temp.g_people").show()
