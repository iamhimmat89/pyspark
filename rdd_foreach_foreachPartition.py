from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Foreach").setMaster("local")
sc = SparkContext(conf=conf)


def func(x):
    print(x)


def func_iter(iterator):
    for x in iterator:
        print(x)


rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.foreach(func)
rdd.foreachPartition(func_iter)
