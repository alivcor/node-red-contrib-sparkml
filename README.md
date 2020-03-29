# node-red-contrib-sparkml

<center><img src="https://github.com/alivcor/node-red-contrib-sparkml/raw/master/images/logo.png" width="300"></center>

This is a Node-RED extension pack and contains a set of nodes which offer Spark Dataframe, SQL and machine learning functionalities. All nodes have a python/pyspark core.

Allows Drag & Drop Machine Learning with Spark. Provides Visual Interface.

## Features

![Drag Drop Spark ML](https://github.com/alivcor/node-red-contrib-sparkml/raw/master/images/drag_drop_sparkml.png "drag_drop_sparkml")

![Choose HyperParameters](https://github.com/alivcor/node-red-contrib-sparkml/raw/master/images/parameters_ui.png "parameters_ui")

![Monitor Progress on Spark](https://github.com/alivcor/node-red-contrib-sparkml/raw/master/images/spark_ui.png "spark_ui")

## Functionalities

This project is a WIP, and I am planning to add more nodes - as many as are available in Spark Transformers and Estimators.

#### Feature Extractors
- [x] TF-IDF
- [ ] Word2Vec
- [x] CountVectorizer
- [ ] FeatureHasher

#### Feature Transformers
- [x] Tokenizer
- [ ] StopWordsRemover
- [ ] n-gram
- [ ] Binarizer
- [ ] PCA
- [x] StringIndexer
- [ ] IndexToString
- [ ] OneHotEncoderEstimator
- [x] VectorIndexer
- [x] SQLTransformer
- [x] VectorAssembler

#### Classification Algorithms
- [x] Decision Tree Classifier
- [x] Logistic Regression
- [x] Gradient-boosted Tree Classifier
- [x] Multilayer Perceptron 
- [x] Random Forest Classifier
- [ ] Support Vector Machines
- [ ] k-Nearest Neighbour Classifier


#### Clustering Algorithms
- [ ] K-Means Clustering
- [ ] Latent Dirichlet allocation (LDA)


## Pre requisites
Be sure to have a working installation of [Node-RED](https://nodered.org/ "Node-RED").  
Install python and the following libraries:
* [Python](https://www.python.org/ "Python") 3.6.4 or higher accessible by the command 'python' (on linux 'python3')
* [PySpark](https://spark.apache.org/ "PySpark")

## Install
To install the latest version use the Menu - Manage palette option and search for node-red-contrib-sparkml, or run the following command in your Node-RED user directory (typically `~/.node-red`):

    npm i node-red-contrib-sparkml


## Usage
These flows create a dataset, train a model and then evaluate it. Models, after training, can be use in real scenarios to make predictions.

There is an example flow and a test dataset available in the 'test' folder. 
  
**Tip:** You can run 'node-red' (or 'sudo node-red' if you are using linux/mac) from the folder '.node-red/node-modules/node-red-contrib-sparkml' to avoid confusion.

Example Deployment
![Deployment](https://github.com/alivcor/node-red-contrib-sparkml/raw/master/images/example_flow.png "Deployment")

## Contributors Welcome 

I am looking for contributors! Feel free to open issues directly on github or email me for any questions, suggesting features or general feedback!
