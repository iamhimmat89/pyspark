from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

conf = SparkConf().setAppName("sql_df_inferring").setMaster("local")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

rdd = sc.textFile("C:\\Users\lenovo\Documents\\tmp\data.txt")
parts = rdd.map(lambda s: s.split(","))

# Inferring
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

teens = spark.sql("select name from people where age >= 13 and age <= 19")
teensNames = teens.rdd.map(lambda p: "Name: " + p.name).collect()
print(teensNames)


# Specifying the schema
people_1 = parts.map(lambda p: (p[0], p[1]))
schemaString = "name age"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
schema = StructType(fields)

schemaPeople_1 = spark.createDataFrame(people_1, schema)
schemaPeople_1.createOrReplaceTempView("people_1")

results = spark.sql("select * from people_1")
results.show()
