from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD textFile").setMaster("local")
sc = SparkContext(conf=conf)

distFile = sc.textFile("C:\\Users\lenovo\Documents\GitHub\python\README.md")
totalfilelength = distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)
print(totalfilelength)
