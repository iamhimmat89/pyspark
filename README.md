# Spark Examples in Python (pyspark)

## **Spark Core**

1.	**SparkContext**

	- 	Parallelized Collections
	- 	External Datasets
		- 	textFile
		- 	wholeTextFile
		-	sequenceFile
	-	addFile 

2.	**Resilient Distributed Datasets (RDDs)**

	-	Passing function to RDD
	-	aggregate
	-	aggregateByKey
	-	cogroup
	-	combineByKey
	-	count, countByKey, countByValue
	-	filter
	-	fold, foldByKey
	-	foreach, foreachPartition
	-	fullOuterJoin
	-	groupBy, groupByKey
	-	join
	-	leftOuterJoin
	-	map, flatMap, mapPartitions
	-	persist
	-	reduce, reduceByKey
	-	repartition, repartitionAndSortWithinPartitions
	-	rightOuterJoin
	-	Set Operations - cartesian, intersection, union
	-	zip
	
	
## **Spark SQL**

1. 	**DataFrame & Datasets**

	- 	DF - JSON
	-	Converting existing RDDs into Datasets
		-	Inferring the schema
		-	Specifying the schema
	-	Load and Save
	-	Bucket, Sort and Partition
	-	DF - Parquet
	-	Register UDF
	-	explain
	-	fillna
	-	select, filter, groupBy
	-	createOrReplaceTempView
	-	createGlobalTempView
	

## **Structured Streaming**

``` 
Note - Use socket source only for testing. The socket source should not be used for production applications!
```

1.	**Structured Streaming word count example in pyspark**

	```
	**Below issue might occur on windows OS: (in socket source)**
	2019-01-19 19:39:29 ERROR MicroBatchExecution:91 - Query [id = e59dc853-eac3-48b4-b161-b6734f35b20d, runId = 7d7b6f57-c8b6-4c87-b008-85a23f52d3e2] terminated with error
	java.net.ConnectException: Connection refused: connect
		at java.net.DualStackPlainSocketImpl.connect0(Native Method)
	```	
	
	```	
	**Solution:**
	Make sure NetCat service has been started before running stream job on specified port. 
	Here, port is 9999 i.e. option("port", "9999")
	
	To check NetCat is installed or not -
	Run below command on cmd:
	nc
	If you see below,
	Cmd line:
	then NetCat is already installed. start service on specified port.
	
	If not then
	1. download NetCat from internet (search for netcat for windows - zip)
	2. extract in C:\ drive
	3. Set windows Environment variable: go to Environment Variables -> System Variables -> Edit Path and and add new path like C:\netcat -> Ok -> Ok
	4. Re-open cmd and check command 'nc'
	
	Start NetCat Service on specified port (on cmd terminal):
	nc -l -p 9999
	
	Now run (using spark-submit or through any editor like pycharm) stream job.
	```	

2.	**Create DF from network streaming input and write to JSON file**
	
	-	Read streams from socket on port 9999
	-	Write streams to \stream\in in csv format
	-	Read streams from \stream\in and perform operations
	-	finally write processed data to \stream\out in JSON format

	```
	**ERROR:**
	2019-01-20 10:32:58 ERROR MicroBatchExecution:91 - Query [id = 69af1bc7-69e2-472d-b8db-627b0f3a6477, runId = 079df7cb-d42e-44a2-b0c5-e0883e55f697] terminated with error
	java.lang.RuntimeException: Offsets committed out of order: 1 followed by 0
	```

	```	
	**Solution:**
	Remove data from checkpint directories before running job. e.g. C:\Users\lenovo\Documents\tmp\stream\ck\ 
	```

3.	**Structured Streaming word count with window operations on event time in pyspark**
	
4.	**Structured Streaming word count with watermarking (handling late data) in pyspark**

