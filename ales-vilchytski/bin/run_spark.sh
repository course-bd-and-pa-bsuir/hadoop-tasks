
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [[ "$1" == "--debug" || "$1" == "-d" ]]; then 
	echo "Running in debug mode"
	START_OPTS=$(cat $DIR/debug_opts)
	shift
  else 
	START_OPTS=""
fi

spark-submit --master "local[1]" --jars hack $DIR/../target/mapreduce-jar-with-dependencies.jar "$@"
