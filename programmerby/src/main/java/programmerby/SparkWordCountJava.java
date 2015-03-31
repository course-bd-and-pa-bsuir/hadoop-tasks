package programmerby;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkWordCountJava {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <master> <input> [<output>]");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setMaster(args[0])
                .setAppName(SparkWordCountJava.class.getSimpleName());
        JavaSparkContext spark = new JavaSparkContext(conf);

        JavaRDD<String> input = spark.textFile(args[1], 1);
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        if (args.length > 2) {
            counts.saveAsTextFile(args[2]);
        } else {
            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<?,?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }
    }
}
