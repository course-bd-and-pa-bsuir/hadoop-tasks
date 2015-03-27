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

public class Problem1 extends Configured implements Tool {
    public static class Problem1Mapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable plus = new IntWritable(1);
        private final static IntWritable minus = new IntWritable(-1);

        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            JsonParserFactory factory = JsonParserFactory.getInstance();
            JSONParser parser = factory.newJsonParser();
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
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > 0) {
                result.set(sum);
                context.write(key, new Text("non-symmetric"));
            }
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (!(args.length != 2 || args.length != 4)) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(Problem1.class);
        job.setMapperClass(Problem1Mapper.class);
        job.setReducerClass(Problem1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Problem1(), args);
        System.exit(exitCode);
    }
}
