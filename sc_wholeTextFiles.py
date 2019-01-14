from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("Whole Text Files").setMaster("local")
sc = SparkContext(conf=conf)

with open(os.path.join("C:\\Users\lenovo\Documents\\tmp", "1.txt"), "w") as file1:
    file1.write("1")

with open(os.path.join("C:\\Users\lenovo\Documents\\tmp", "2.txt"), "w") as file2:
    file2.write("2")

textFiles = sc.wholeTextFiles("C:\\Users\lenovo\Documents\\tmp")
data = sorted(textFiles.collect())
print(data)
