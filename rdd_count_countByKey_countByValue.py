from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Count").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
print(rdd.count())
print(rdd.countByKey())
print(rdd.countByValue())
