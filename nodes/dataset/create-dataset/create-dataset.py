import json
import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit

#read configurations
config = json.loads(input())
# print(config)

while True:
	#wait request
	input()

	# print("Starting SparkSession")
	spark = SparkSession.builder.master(config['sparkMaster']).appName(config['sparkApp']).getOrCreate()

	# Prepare training and test data.
	data = spark.read.load(config['scheme'] + "://" + config['path'], format=config['fileType'], sep=config['separator'], inferSchema=config['inferSchema'], header=config['readHeaders'])
	train, test = data.randomSplit([config['trainingPartition'], 1-config['trainingPartition']], seed=config['seed'])

	if not os.path.isdir(config['save']):
		os.makedirs(config['save'], exist_ok=True)

	if "partitionCol" in config and config['partitionCol'] in data.schema.names:
		train.write.partitionBy(config['partitionCol']).format("parquet").save(config['scheme'] + "://" + config['save'] + "/train/")
		test.write.partitionBy(config['partitionCol']).format("parquet").save(config['scheme'] + "://" + config['save'] + "/test/")
	else:
		train.write.format("parquet").mode("overwrite").save(config['scheme'] + "://" + config['save'] + "/train/")
		test.write.format("parquet").mode("overwrite").save(config['scheme'] + "://" + config['save'] + "/test/")
	spark.stop()
	# print('Dataset created.')
	# print("\n\n")
	config["currentTrain"] = config['scheme'] + "://" + config['save'] + "/train/"
	config["currentTest"] = config['scheme'] + "://" + config['save'] + "/test/"
	print(json.dumps(config))
