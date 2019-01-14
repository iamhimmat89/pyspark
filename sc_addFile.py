from pyspark import SparkContext, SparkConf, SparkFiles
import os

conf = SparkConf().setAppName("Add File").setMaster("local")
sc = SparkContext(conf=conf)

path = os.path.join("C:\\Users\lenovo\Documents\\tmp", "test.txt")
with open(path, "w") as testFile:
    testFile.write("100")

sc.addFile(path)


def func(lst):
    with open(SparkFiles.get("test.txt")) as file:
        val = int(file.readline())
        return [x * val for x in lst]


output = sc.parallelize([1, 2, 3, 4, 5]).mapPartitions(func).collect()
print(output)
