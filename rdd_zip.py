from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Zip").setMaster("local")
sc = SparkContext(conf=conf)

rdd1 = sc.parallelize(range(0,5))
rdd2 = sc.parallelize(range(1000, 1005))
print(rdd1.zip(rdd2).collect())
