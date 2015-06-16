#!/bin/bash
mvn clean package
HADOOP_VERSION=1.2.1
JAR_PATH=./target/wordcount.jar
HADOOP_HOME=/Users/donglei/Desktop/workspace/04Demo/hadoop-${HADOOP_VERSION}
INPUT=./input
OUTPUT=./output
$HADOOP_HOME/bin/hadoop jar $JAR_PATH MyWordCount $INPUT $OUTPUT
