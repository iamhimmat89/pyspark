from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD aggregate").setMaster("local")
sc = SparkContext(conf=conf)

listRDD = sc.parallelize([1, 2, 3, 4, 5, 6], 2)
seqOp = (lambda loc_res, list_val: (loc_res[0] + list_val, loc_res[1] + 1))
combOp = (lambda some_loc_res, another_loc_res: (some_loc_res[0] + another_loc_res[0], some_loc_res[1] + another_loc_res[1]))

result = listRDD.aggregate((0, 0), seqOp, combOp)
print(result)
