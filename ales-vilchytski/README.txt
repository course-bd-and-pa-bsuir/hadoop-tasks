To build use Maven2:

$ mvn package

To run use script in bin-folder:

$ bin/run_local.sh <spark|hadoop>-<relations|sqlike|text> <in> <out>

e.g. $ bin/run_local.sh spark-relations data/in/rel data/out
