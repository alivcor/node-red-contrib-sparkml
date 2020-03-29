#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import pyspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplit


config = json.loads(input())

while True:
    data = input()
    if data:
        spark = SparkSession.builder.master(json.loads(data)['sparkMaster']).appName(json.loads(data)['sparkApp']).getOrCreate()
        df = spark.read.parquet(json.loads(data)['predictionsPath'])
        df.select(*[x.strip() for x in config['exportColumns'].split(",")]).coalesce(1).write.csv(config['csvPath'])

        spark.stop()

        # print('Dataset Loaded')

        print(json.dumps(json.loads(data)))


