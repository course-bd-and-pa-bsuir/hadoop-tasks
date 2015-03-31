/**
 * Problem 1: In social networks, the he relationship friend is often symmetric. That implies that if Bob is friend with Amy, Amy is friend with Bob. For this exercise, implement a MapReduce algorithm to determine whether or not this property holds true for the provided sample data. In other words, the MapReduce application has to produce a list of all non-symmetric friend relationships. In other words, find all the friend relationships where (as an example) Bob is friend with Alex, but Alex is not friend with Bob or Mike is friend with Coney, but Coney is not friend with Mike.
 A JSON file links.json is provided that can be used as the input into the Map function. The output of the Reduce function has to be the list of non-symmetric friend records
 */
package programmerby;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

import com.codesnippets4all.json.parsers.JSONParser;
import com.codesnippets4all.json.parsers.JsonParserFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopProblem1 extends Configured implements Tool {
    public static class Problem1Mapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable plus = new IntWritable(1);
        private final static IntWritable minus = new IntWritable(-1);

        private JSONParser parser = JsonParserFactory.getInstance().newJsonParser();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map jsonData = parser.parseJson(value.toString());
            ArrayList names = (ArrayList)jsonData.get("root");

            word.set(names.get(0) + " " + names.get(1));
            context.write(word, plus);

            word.set(names.get(1) + " " + names.get(0));
            context.write(word, minus);
        }
    }

    public static class Problem1Reducer extends
            Reducer<Text, IntWritable, Text, Text> {
        private final static Text result = new Text("non-symmetric");

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > 0) {
                context.write(key, result);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            System.err.println("Usage: <input_path> <output_path_noexist>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(HadoopProblem1.class);
        job.setMapperClass(Problem1Mapper.class);
        job.setReducerClass(Problem1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopProblem1(), args);
        System.exit(exitCode);
    }
}
