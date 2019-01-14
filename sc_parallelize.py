from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Parallelism").setMaster("local")
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distDataSum = distData.reduce(lambda a, b: a + b)
print(distDataSum)
