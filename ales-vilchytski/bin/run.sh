#!/bin/bash

if [[ "$1" == "--debug" || "$1" == "-d" ]]; then 
	echo "Running in debug mode"
	START_OPTS=$(cat debug_opts)
	shift
  else 
	START_OPTS=""
fi

HADOOP_OPTS="$START_OPTS" hadoop jar ../target/mapreduce.jar "$@"
