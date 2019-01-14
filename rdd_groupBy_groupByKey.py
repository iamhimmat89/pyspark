from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD GroupBy").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7])
result = rdd.groupBy(lambda e: e % 2).collect()
for x, y in result:
    print(x, list(y))

rdd1 = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
print(rdd1.groupByKey().mapValues(list).collect())
