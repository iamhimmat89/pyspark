from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("RDD Reduce").setMaster("local")
sc = SparkContext(conf=conf)

print(sc.parallelize([1, 2, 3, 4, 5]).reduce(add))
print(sc.parallelize([("a", 1), ("b", 1), ("a", 1)]).reduceByKey(add).collect())
