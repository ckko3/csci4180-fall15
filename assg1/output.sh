#!/bin/bash

if [ $# -eq 2 ]; then
	hadoop dfs -cat $1/$2/part-r-00000
else
	echo "Example: $0 wordcount hello"
fi
