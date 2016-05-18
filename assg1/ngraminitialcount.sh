#!/bin/bash

if [ $# -eq 2 ]; then
	IN=input/$1
	OUT=ngraminitialcount/$1
	
	hadoop dfs -ls $OUT > /dev/null 2>&1
	if [ $? -eq 0 ]; then
		hadoop dfs -rmr $OUT > /dev/null 2>&1
	fi
	hadoop jar ngraminitialcount.jar ngram.NgramInitialCount $IN $OUT $2
else
	echo "Example: $0 q3 2"
fi
