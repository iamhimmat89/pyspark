from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql udf").getOrCreate()

strlen = udf(lambda x: len(x), IntegerType())
_ = spark.udf.register("strlen", strlen)
result = spark.sql("select strlen('test')").collect()

print(result)

