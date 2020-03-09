# node-red-contrib-sparkml
![Logo]( "Logo")

This is a Node-RED extension pack and contains a set of nodes which offer Spark Dataframe, SQL and machine learning functionalities. All nodes have a python/pyspark core.

Allows Drag & Drop Machine Learning with Spark. Provides Visual Interface.


## Pre requisites
Be sure to have a working installation of [Node-RED](https://nodered.org/ "Node-RED").  
Install python and the following libraries:
* [Python](https://www.python.org/ "Python") 3.6.4 or higher accessible by the command 'python' (on linux 'python3')
* [PySpark](https://spark.apache.org/ "PySpark")

## Install
To install the latest version use the Menu - Manage palette option and search for node-red-contrib-sparkml, or run the following command in your Node-RED user directory (typically ~/.node-red):

    npm i node-red-contrib-sparkml

## Usage
These flows create a dataset, train a model and then evaluate it. Models, after training, can be use in real scenarios to make predictions.

There is an example flow and a test dataset available in the 'test' folder. 
  
**Tip:** You can run 'node-red' (or 'sudo node-red' if you are uning linux/mac) from the folder '.node-red/node-modules/node-red-contrib-sparkml' to avoid confusion.

Example Deployment
![Deployment]( "Deployment")

