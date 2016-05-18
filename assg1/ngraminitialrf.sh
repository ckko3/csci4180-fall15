#!/bin/bash

if [ $# -eq 3 ]; then
	IN=input/$1
	OUT=ngraminitialrf/$1
	
	hadoop dfs -ls $OUT > /dev/null 2>&1
	if [ $? -eq 0 ]; then
		hadoop dfs -rmr $OUT > /dev/null 2>&1
	fi
	hadoop jar ngraminitialrf.jar ngram.NgramInitialRF $IN $OUT $2 $3
else
	echo "Example: $0 q4 2 0.6"
fi
