import json
import pyspark
import os, sys
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import SQLTransformer
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

			trainPath = data['scheme'] + "://" + data['save'] + "/trainTokenized/"
			testPath = data['scheme'] + "://" + data['save'] + "/testTokenized/"

			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				trainTokenized.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				testTokenized.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				trainTokenized.write.format("parquet").mode("overwrite").save(trainPath)
				testTokenized.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()

			data["trainTokenized"] = trainPath
			data["testTokenized"] = testPath
			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "tf":

			trainTokenized, testTokenized = spark.read.parquet(data["trainTokenized"]), spark.read.parquet(data["testTokenized"])
			trainTokenized.cache()
			testTokenized.cache()

			hashingTF = HashingTF(inputCol=config["inputCol"], outputCol=config["outputCol"], numFeatures=20)
			trainTf = hashingTF.transform(trainTokenized)
			testTf = hashingTF.transform(testTokenized)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainTf/"
			testPath = data['scheme'] + "://" + data['save'] + "/testTf/"

			if "partitionCol" in data and data['partitionCol'] in trainTf.schema.names:
				trainTf.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				testTf.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				trainTf.write.format("parquet").mode("overwrite").save(trainPath)
				testTf.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()
			data["trainTf"] = trainPath
			data["testTf"] = testPath
			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "idf":

			trainTf, testTf = spark.read.parquet(data["trainTf"]), spark.read.parquet(data["testTf"])
			trainTf.cache()
			testTf.cache()

			idf = IDF(inputCol=config["inputCol"], outputCol=config["outputCol"])
			idfModel = idf.fit(trainTf)
			trainIdf = idfModel.transform(trainTf)
			testIdf = idfModel.transform(testTf)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainIdf/"
			testPath = data['scheme'] + "://" + data['save'] + "/testIdf/"

			if "partitionCol" in data and data['partitionCol'] in trainIdf.schema.names:
				trainIdf.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				testIdf.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				trainIdf.write.format("parquet").mode("overwrite").save(trainPath)
				testIdf.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()
			data["trainIdf"] = trainPath
			data["testIdf"] = testPath

			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "si":

			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])
			train.cache()
			test.cache()

			df = train.unionByName(test)
			labelIndexer = StringIndexer(inputCol=config["inputCol"], outputCol=config["outputCol"]).fit(df)
			train = labelIndexer.transform(train)
			test = labelIndexer.transform(test)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainSI/"
			testPath = data['scheme'] + "://" + data['save'] + "/testSI/"
			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				train.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				test.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				train.write.format("parquet").mode("overwrite").save(trainPath)
				test.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()
			data["trainSI"] = trainPath
			data["testSI"] = testPath

			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "vi":

			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])
			train.cache()
			test.cache()

			df = train.unionByName(test)
			featureIndexer = VectorIndexer(inputCol=config["inputCol"], outputCol=config["outputCol"], maxCategories=config["maxCategories"]).fit(df)
			train = featureIndexer.transform(train)
			test = featureIndexer.transform(test)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainVI/"
			testPath = data['scheme'] + "://" + data['save'] + "/testVI/"
			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				train.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				test.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				train.write.format("parquet").mode("overwrite").save(trainPath)
				test.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()

			data["trainVI"] = trainPath
			data["testVI"] = testPath

			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "va":

			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])
			train.cache()
			test.cache()

			df = train.unionByName(test)
			inputCols = [x.strip() for x in config["inputCols"].split(",")]
			assembler = VectorAssembler(inputCols=inputCols, outputCol=config["outputCol"]) 
			train = assembler.transform(train)
			test = assembler.transform(test)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainVA/"
			testPath = data['scheme'] + "://" + data['save'] + "/testVA/"
			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				train.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				test.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				train.write.format("parquet").mode("overwrite").save(trainPath)
				test.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()

			data["trainVA"] = trainPath
			data["testVA"] = testPath

			data["currentTrain"] = trainPath
			data["currentTest"] = testPath

		elif config["transformerType"] == "sql":

			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])
			train.cache()
			test.cache()

			df = train.unionByName(test)
			sqlTrans = SQLTransformer(statement=config["statement"])
			train = sqlTrans.transform(train)
			test = sqlTrans.transform(test)

			trainPath = data['scheme'] + "://" + data['save'] + "/trainSQL/"
			testPath = data['scheme'] + "://" + data['save'] + "/testSQL/"
			if "partitionCol" in data and data['partitionCol'] in train.schema.names:
				train.write.partitionBy(data['partitionCol']).format("parquet").save(trainPath)
				test.write.partitionBy(data['partitionCol']).format("parquet").save(testPath)
			else:
				train.write.format("parquet").mode("overwrite").save(trainPath)
				test.write.format("parquet").mode("overwrite").save(testPath)
			spark.stop()

			data["trainSQL"] = trainPath
			data["testSQL"] = testPath

			data["currentTrain"] = trainPath
			data["currentTest"] = testPath
		print(json.dumps(data))
	# else:
	# 	print('Nothing to load.', file=sys.stderr)