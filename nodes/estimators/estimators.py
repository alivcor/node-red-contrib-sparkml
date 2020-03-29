import json
import pyspark
import os, sys
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

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

		if config["estimatorType"] == "lr":
			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])

			train.cache()
			test.cache()
			
			lr = LogisticRegression(maxIter=config["maxIter"], regParam=config["regParam"], elasticNetParam=config["elasticNetParam"])

			# Fit the model
			lrModel = lr.fit(train)

			predictions = lrModel.transform(test)

			predictionsPath = data['scheme'] + "://" + data['save'] + "/predictions/"


			if "partitionCol" in data and data['partitionCol'] in predictions.schema.names:
				test.write.partitionBy(data['partitionCol']).format("parquet").save(predictionsPath)
			else:
				predictions.write.format("parquet").mode("overwrite").save(predictionsPath)
			spark.stop()

			data["predictionsPath"] = predictionsPath

		elif config["estimatorType"] == "dtc":
			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])

			train.cache()
			test.cache()
			
			classifier = DecisionTreeClassifier()

			# Fit the model
			model = classifier.fit(train)

			predictions = model.transform(test)

			predictionsPath = data['scheme'] + "://" + data['save'] + "/predictions/"


			if "partitionCol" in data and data['partitionCol'] in predictions.schema.names:
				test.write.partitionBy(data['partitionCol']).format("parquet").save(predictionsPath)
			else:
				predictions.write.format("parquet").mode("overwrite").save(predictionsPath)
			spark.stop()

			data["predictionsPath"] = predictionsPath

		elif config["estimatorType"] == "rfc":
			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])

			train.cache()
			test.cache()
			
			classifier = RandomForestClassifier(numTrees=config["numTrees"])

			# Fit the model
			model = classifier.fit(train)

			predictions = model.transform(test)

			predictionsPath = data['scheme'] + "://" + data['save'] + "/predictions/"


			if "partitionCol" in data and data['partitionCol'] in predictions.schema.names:
				test.write.partitionBy(data['partitionCol']).format("parquet").save(predictionsPath)
			else:
				predictions.write.format("parquet").mode("overwrite").save(predictionsPath)
			spark.stop()

			data["predictionsPath"] = predictionsPath

		elif config["estimatorType"] == "gbt":
			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])

			train.cache()
			test.cache()
			
			classifier = GBTClassifier()

			# Fit the model
			model = classifier.fit(train)

			predictions = model.transform(test)

			predictionsPath = data['scheme'] + "://" + data['save'] + "/predictions/"


			if "partitionCol" in data and data['partitionCol'] in predictions.schema.names:
				test.write.partitionBy(data['partitionCol']).format("parquet").save(predictionsPath)
			else:
				predictions.write.format("parquet").mode("overwrite").save(predictionsPath)
			spark.stop()

			data["predictionsPath"] = predictionsPath

		elif config["estimatorType"] == "mpc":
			train, test = spark.read.parquet(data["currentTrain"]), spark.read.parquet(data["currentTest"])

			train.cache()
			test.cache()
			
			classifier = MultilayerPerceptronClassifier(maxIter=config["maxIter"], layers=[int(x.strip()) for x in config["layers"].split(",")], blockSize=config["blockSize"], seed=config["seed"])

			# Fit the model
			model = classifier.fit(train)

			predictions = model.transform(test)

			predictionsPath = data['scheme'] + "://" + data['save'] + "/predictions/"


			if "partitionCol" in data and data['partitionCol'] in predictions.schema.names:
				test.write.partitionBy(data['partitionCol']).format("parquet").save(predictionsPath)
			else:
				predictions.write.format("parquet").mode("overwrite").save(predictionsPath)
			spark.stop()

			data["predictionsPath"] = predictionsPath



		print(json.dumps(data))
	# else:
	# 	print('Nothing to load.', file=sys.stderr)