from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Set Operations").setMaster("local")
sc = SparkContext(conf=conf)

listRDD = sc.parallelize([1, 2, 3, 4, 5, 6])
cartRDD = listRDD.cartesian(listRDD).collect()
print(cartRDD)

listRDD2 = sc.parallelize([2, 3, 6, 7, 9])
print(listRDD.intersection(listRDD2).collect())

print(listRDD.union(listRDD2).collect())
