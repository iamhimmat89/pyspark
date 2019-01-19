from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

conf = SparkConf().setAppName("sql_parquet").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

df = spark.read.json("C:\\Users\lenovo\Documents\\tmp\data.json")
df.write.save("data.parquet")

parquetFile = spark.read.parquet("data.parquet")
parquetFile.createOrReplaceTempView("parquetFile")

teens = spark.sql("select * from parquetFile where age >= 13 and age <= 19")
teens.show()

squareDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda x: Row(single=x, double=x**2)))
squareDF.write.parquet("C:\\Users\lenovo\Documents\\tmp\\test_table\key=1")

cubeDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda x: Row(single=x, triple=x**3)))
cubeDF.write.parquet("C:\\Users\lenovo\Documents\\tmp\\test_table\key=2")

mergeDF = spark.read.option("mergeSchema", "true").parquet("C:\\Users\lenovo\Documents\\tmp\\test_table")
mergeDF.printSchema()
