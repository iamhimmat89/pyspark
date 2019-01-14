from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD sequenceFile").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.saveAsSequenceFile("C:\\Users\lenovo\Documents\sequenceFile")
seqFileOutput = sorted(sc.sequenceFile("C:\\Users\lenovo\Documents\sequenceFile").collect())
print(seqFileOutput)
