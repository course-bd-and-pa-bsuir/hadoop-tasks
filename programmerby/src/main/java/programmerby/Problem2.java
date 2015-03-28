/**
 * Problem 2: Develop a MapReduce application that generates an inverted index. To illustrate, given a set of documents, an inverted index reflects a dictionary where each word is associated with a list of the document identifiers in which that word appears. In other words, the output of the Reduce function should be a list of words and the associated documents where that word can be found. As an example, if the word Texas can be found in documents A, B, and D, the output of the Reduce function should be:
 Texas -> [documentA, documentB, documentD]
 A Jason file text.json is provide. All the words in the text have to be referenced in your solution.
 */
package programmerby;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.codesnippets4all.json.parsers.JSONParser;
import com.codesnippets4all.json.parsers.JsonParserFactory;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

public class Problem2 extends Configured implements Tool {
    public static class Problem2Mapper extends
            Mapper<Object, Text, Text, Text> {
        private JSONParser parser = JsonParserFactory.getInstance().newJsonParser();

        private Text word = new Text();
        private Text doc = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map jsonData = parser.parseJson(value.toString());
            ArrayList kv = (ArrayList)jsonData.get("root");

            String[] words = kv.get(1).toString().split(" ");

            doc.set(kv.get(0).toString());
            for (String w : words) {
                word.set(w);
                context.write(word, doc);
            }
        }
    }

    public static class Problem2Reducer extends
            Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> docs = new TreeSet<String>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            docs.clear();
            for (Text val : values) {
                docs.add(val.toString());
            }

            result.set("[" + StringUtils.join(docs, ", ") + "]");
            context.write(key, result);
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

        job.setJarByClass(Problem2.class);
        job.setMapperClass(Problem2Mapper.class);
        job.setReducerClass(Problem2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Problem2(), args);
        System.exit(exitCode);
    }
}
