import json
import pyspark
import os, sys
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit

#read configurations
config = json.loads(input())

while True:
	#wait request
	data = input()
	if data:
		# print("Loading Dataset")

		# print(" Spark Master : " + json.loads(data)['sparkMaster'])
		spark = SparkSession.builder.master(json.loads(data)['sparkMaster']).appName(json.loads(data)['sparkApp']).getOrCreate()

		datasetPath = json.loads(data)['scheme'] + "://" + json.loads(data)['save'] 
		# print(" Dataset Path : " + datasetPath)
		# Prepare training and test data.
		train, test = spark.read.parquet(datasetPath + "/train"), spark.read.parquet(datasetPath + "/test")

		train.cache()
		test.cache()
		spark.stop()

		# print('Dataset Loaded')
		
		print(json.dumps(json.loads(data)))
	# else:
	# 	print('Nothing to load.', file=sys.stderr)