from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql explain").getOrCreate()

parquetFile = spark.read.parquet("data.parquet")
parquetFile.createOrReplaceTempView("parquetFile")

teens = spark.sql("select * from parquetFile where age >= 13 and age <= 19")
teens.explain(True)
