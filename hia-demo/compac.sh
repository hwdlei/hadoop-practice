#!/bin/bash
HADOOP_VERSION=1.2.1
HADOOP_HOME=/Users/donglei/Desktop/workspace/04Demo/hadoop-${HADOOP_VERSION}
LIB=${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar
echo ${LIB}
mkdir bin
javac -cp $LIB -d bin *.java
jar -cvf wordcount.jar -C bin/ .
