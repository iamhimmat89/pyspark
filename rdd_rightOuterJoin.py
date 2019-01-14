from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD RightOuterJoin").setMaster("local")
sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([("a", 1), ("b", 4)])
rdd2 = sc.parallelize([("a", 2), ("c", 5)])

print(rdd1.rightOuterJoin(rdd2).collect())
