from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD combine by key").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])


def to_list(a):
    return [a]


def append(a, b):
    a.append(b)
    return a


def extend(a, b):
    a.extend(b)
    return a


result = sorted(rdd.combineByKey(to_list, append, extend).collect())
print(result)
