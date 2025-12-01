# VS-DBSCAN

VS-DBSCAN: A Distributed Framework of DBSCAN for High-Dimensional Vector Streams


## Introduction
  This folder (i.e., ./src) holds the source codes in our paper, VS-DBSCAN: A Distributed Framework of DBSCAN for High-Dimensional Vector Streams


## Environment Preparation
  - Flink version: 1.19.0
  - A cluster containing 10 nodes, where each node is equipped with two 12-core processors (Intel Xeon E-5-2620 v3 2.40 GHz), 128GB RAM
  - System version: Ubuntu 22.04
  - Java version: 1.8.0
  - Please refer to the source code to install all required packages of libs by Maven.
  

## Running 
  - Import the selected project to Intellij IDEA；
  - Downloading all required dependences by Maven; 
  - Package the project to a X.jar, where X is your project name；
  - Load the packaged X.jar to the master node of your Flink cluster；
  - Running your project in a cluster environment.
