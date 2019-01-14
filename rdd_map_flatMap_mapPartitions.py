from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Map").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([2, 3, 4, 5], 2)
print(sorted(rdd.map(lambda x: (1, x)).collect()))
print(sorted(rdd.flatMap(lambda x: (1, x)).collect()))


def func(iterator): yield sum(iterator)


print(rdd.mapPartitions(func).collect())
