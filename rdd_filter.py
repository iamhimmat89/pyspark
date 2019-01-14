from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Filter").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(rdd.filter(lambda x: x % 2 == 0).collect())
