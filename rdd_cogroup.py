from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD cogroup").setMaster("local")
sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([("a", 1), ("b", 3)])
rdd2 = sc.parallelize([("a", 2)])

result = sorted(list(rdd1.cogroup(rdd2).collect()))
for x, y in result:
    print(x, tuple(map(list, y)))


