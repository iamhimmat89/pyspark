from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("RDD Fold").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.fold(0, add))
print(rdd.map(lambda x: ('a', x)).foldByKey(0, add).collect())
