#!/bin/bash

if [ $# -eq 2 ]; then
	javac -classpath .:$HADOOP_HOME/hadoop-core-0.20.203.0.jar $1 -d $2
	if [ $? -eq 0 ]; then
		jar -cvf $2.jar -C $2/ .
	fi
else
	echo "Example: $0 WordCount.java wordcount"
fi
