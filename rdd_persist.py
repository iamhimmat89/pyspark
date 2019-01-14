from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Persist").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile("C:\\Users\lenovo\Documents\GitHub\python\README.md")
lineLengths = lines.map(lambda s: len(s))
lineLengths.persist()
totalLength = lineLengths.reduce(lambda a, b: a + b)
print(totalLength)
