import json
import pyspark
import os, sys
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
#read configurations
config = json.loads(input())

while True:
	#wait request
	data = input()

	if data:
		# print("Loading Dataset")
		# print(data)
		data = json.loads(data)
		spark = SparkSession.builder.master(data['sparkMaster']).appName(data['sparkApp']).getOrCreate()

		datasetPath = data['scheme'] + "://" + data['save'] 
		# print(" Dataset Path : " + datasetPath)
		# Prepare training and test data.

		if config["transformerType"] == "tokenizer":
			train, test = spark.read.parquet(datasetPath + "/train"), spark.read.parquet(datasetPath + "/test")

			train.cache()
			test.cache()
			tokenizer = Tokenizer(inputCol=config["inputCol"], outputCol=config["outputCol"])
			trainTokenized = tokenizer.transform(train)
			testTokenized = tokenizer.transform(test)


			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				trainTokenized.write.partitionBy(data['partitionCol']).format("parquet").save(data['scheme'] + "://" + data['save'] + "/trainTokenized/")
				testTokenized.write.partitionBy(data['partitionCol']).format("parquet").save(data['scheme'] + "://" + data['save'] + "/testTokenized/")
			else:
				trainTokenized.write.format("parquet").mode("overwrite").save(data['scheme'] + "://" + data['save'] + "/trainTokenized/")
				testTokenized.write.format("parquet").mode("overwrite").save(data['scheme'] + "://" + data['save'] + "/testTokenized/")
			spark.stop()

			data["trainTokenized"] = data['scheme'] + "://" + data['save'] + "/trainTokenized/"
			data["testTokenized"] = data['scheme'] + "://" + data['save'] + "/testTokenized/"

		elif config["transformerType"] == "tf":

			trainTokenized, testTokenized = spark.read.parquet(data["trainTokenized"]), spark.read.parquet(data["testTokenized"])
			trainTokenized.cache()
			testTokenized.cache()

			hashingTF = HashingTF(inputCol=config["inputCol"], outputCol=config["outputCol"], numFeatures=20)
			trainTf = hashingTF.transform(trainTokenized)
			testTf = hashingTF.transform(testTokenized)

			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				trainTf.write.partitionBy(data['partitionCol']).format("parquet").save(data['scheme'] + "://" + data['save'] + "/trainTf/")
				testTf.write.partitionBy(data['partitionCol']).format("parquet").save(data['scheme'] + "://" + data['save'] + "/testTf/")
			else:
				trainTf.write.format("parquet").mode("overwrite").save(data['scheme'] + "://" + data['save'] + "/trainTf/")
				testTf.write.format("parquet").mode("overwrite").save(data['scheme'] + "://" + data['save'] + "/testTf/")
			spark.stop()

		print(json.dumps(data))
	# else:
	# 	print('Nothing to load.', file=sys.stderr)