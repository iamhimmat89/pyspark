from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("RDD Repartition").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
print(rdd.repartition(2).glom().collect())

rdd1 = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
print(rdd1.repartitionAndSortWithinPartitions(2, lambda x: x % 2, True).glom().collect())
