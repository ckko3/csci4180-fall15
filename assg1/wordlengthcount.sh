#!/bin/bash

if [ $# -eq 1 ]; then
	IN=input/$1
	OUT=wordlengthcount/$1
	
	hadoop dfs -ls $OUT > /dev/null 2>&1
	if [ $? -eq 0 ]; then
		hadoop dfs -rmr $OUT > /dev/null 2>&1
	fi
	hadoop jar wordlengthcount.jar ngram.WordLengthCount $IN $OUT
else
	echo "Example: $0 q2"
fi
