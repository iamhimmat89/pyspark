from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Join").setMaster("local")
sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", 2), ("a", 3)])

print(rdd1.join(rdd2).collect())
