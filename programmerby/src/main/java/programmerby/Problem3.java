/**
 * SQL-like query Consider the query:

 SELECT *
 FROM Orders, LineItem
 WHERE Order.order_id = LineItem.order_id

 Your MapReduce query should produce the same information as the SQL query. You can consider the 2 input tables, Order and LineItem, as one big concatenated bag of records which is passed into the Map function (record by record). Hence, the Map input reflects the database records formatted as lists of string objects. Every list element corresponds to a different field in its corresponding record. The first item (index 0) in each record represents a string that identifies which table the record originates from. This field has 2 possible values:
 - line_item -> indicates that the record is a line item.
 - order -> indicates that the record is an order.

 The second element (index 1) in each record is the order_id. The LineItem records consist of 17 elements, including the identifier string. Order records consists of 10 elements, including the identifier string.
 The Reduce output should reflect a joined record. The result should be a single list of length 27 that contains the fields from the order record followed by the fields from the line item record. Each list element should be a string.

 A JSON file sqllike.json is provided and can be used as the input into the Map function.
 */
package programmerby;

import java.io.*;
import java.util.*;

import com.codesnippets4all.json.generators.JSONGenerator;
import com.codesnippets4all.json.generators.JsonGeneratorFactory;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Problem3 extends Configured implements Tool {
    public static class Problem3Mapper extends
            Mapper<Object, Text, Text, Text> {
        private JSONParser parser = JsonParserFactory.getInstance().newJsonParser();
        private Text tid = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map jsonData = parser.parseJson(value.toString());
            ArrayList arr = (ArrayList)jsonData.get("root");

            String id = arr.get(1).toString();

            tid.set(id);
            context.write(tid, value);
        }
    }

    public static class Problem3Reducer extends
            Reducer<Text, Text, Text, NullWritable> {
        private Text result = new Text();
        private JSONParser parser = JsonParserFactory.getInstance().newJsonParser();
        private JSONGenerator generator = JsonGeneratorFactory.getInstance().newJsonGenerator();
        private NullWritable nullValue = NullWritable.get();
        private LinkedList<ArrayList> tmp = new LinkedList<ArrayList>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            ArrayList orderArr = null;
            tmp.clear();

            // What should we do if we have >100000 of line_items??......
            for (Text value : values) {
                Map jsonData = parser.parseJson(value.toString());
                ArrayList arr = (ArrayList)jsonData.get("root");

                if (arr.get(0).toString().equals("order")) {
                    orderArr = arr;
                    break;
                } else {
                    tmp.add(arr);
                }
            }

            if (orderArr == null) {
                return;
            }

            for (ArrayList arr : tmp) {
                result.set(toJSON(orderArr, arr));
                context.write(result, nullValue);
            }

            // Continue to iterate over values
            for (Text value : values) {
                Map jsonData = parser.parseJson(value.toString());
                ArrayList arr = (ArrayList)jsonData.get("root");

                result.set(toJSON(orderArr, arr));
                context.write(result, nullValue);
            }
        }

        public String toJSON(ArrayList arr1, ArrayList arr2) {
            return StringUtils.substring(generator.generateJson(arr1), 1, -2) + "," + StringUtils.substring(generator.generateJson(arr2), 2, -1);
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

        job.setJarByClass(Problem3.class);
        job.setMapperClass(Problem3Mapper.class);
        job.setReducerClass(Problem3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Problem3(), args);
        System.exit(exitCode);
    }
}
