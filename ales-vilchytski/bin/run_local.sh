#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [[ "$1" == "--debug" || "$1" == "-d" ]]; then 
	echo "Running in debug mode"
	START_OPTS=$(cat $DIR/debug_opts)
	shift
else 
	START_OPTS=""
fi

if [[ "$1" == hadoop-* ]]; then
	echo "Starting hadoop task"
	
	HADOOP_OPTS="$START_OPTS" hadoop jar $DIR/../target/mapreduce-jar-with-dependencies.jar "$@"

elif [[ "$1" == spark-* ]]; then
	echo "Starting spark task"
	
	# Note --jars arguments - for spark 1.3.0 extra non-existing jar is needed due to some strange magic...
	spark-submit --driver-java-options "$START_OPTS" --master "local[1]" --jars hack $DIR/../target/mapreduce-jar-with-dependencies.jar "$@"
else
    echo "Task type in argument '$1' is not recognized"
	exit -1
fi
