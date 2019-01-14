from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD aggregateByKey").setMaster("local")
sc = SparkContext(conf=conf)

studRDD = sc.parallelize([
    ("A", "Data Structure", 70), ("A", "Algorithms", 80), ("A", "Python", 75), ("A", "Spark", 90),
    ("B", "Data Structure", 98), ("B", "Algorithms", 90), ("B", "Python", 83), ("B", "Spark", 54),
    ("C", "Data Structure", 67), ("C", "Algorithms", 65), ("C", "Python", 46), ("C", "Spark", 83),
    ("D", "Data Structure", 84), ("D", "Algorithms", 54), ("D", "Python", 89), ("D", "Spark", 22),
    ("E", "Data Structure", 72), ("E", "Algorithms", 69), ("E", "Python", 72), ("E", "Spark", 84),
    ("F", "Data Structure", 41), ("F", "Algorithms", 81), ("F", "Python", 93), ("F", "Spark", 74)
], 2)


def seq_func(acc, element):
    if acc > element[1]:
        return acc
    else:
        return element[1]


def comb_func(acc1, acc2):
    if acc1 > acc2:
        return acc1
    else:
        return acc2


result = studRDD.map(lambda s: (s[0], (s[1], s[2]))).aggregateByKey(0, seq_func, comb_func).collect()
print(result)
