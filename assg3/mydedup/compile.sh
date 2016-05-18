#!/bin/bash

javac -cp .:azure/*:mapdb-master/src/main/java @source.txt -d build
