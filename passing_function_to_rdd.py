from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD Passing Function").setMaster("local")
sc = SparkContext(conf=conf)

# Using Lambda
distFile = sc.textFile("C:\\Users\lenovo\Documents\GitHub\python\README.md")
fileLength = distFile.map(lambda s: len(s)).reduce(lambda x, y: x + y)
print(fileLength)


# Pass with reference - Don't use this
class LengthFinder(object):
    def __init__(self):
        self.field = "Python"

    def findLength(self, rdd):
        return rdd.filter(lambda s: self.field in s).map(lambda s: len(s)).reduce(lambda x, y: x + y)


lf = LengthFinder()
length = lf.findLength(distFile)
print(length)


# Pass without reference
class LengthFind(object):
    def __init__(self):
        self.field = "Python"

    def findLen(self, rdd):
        field = self.field
        return rdd.filter(lambda s: field in s).map(lambda s: len(s)).reduce(lambda x, y: x + y)


lfind = LengthFind()
filelen = lfind.findLen(distFile)
print(filelen)
