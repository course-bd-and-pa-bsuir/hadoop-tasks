package by.bsuir.course.bdpa.tasks;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 *  Task:
 *  Given json-like file in format 
 *    [ "A", "B" ]
 *    [ "C", "A" ]
 *    ...
 *  
 *  Find out all pairs, which has relations like 'A -> B' or 'B -> A' but not both.
 */
public class SimpleMapReduceRelations {

	public static class SimpleMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			JsonReader rdr = Json.createReader(new StringReader(value.toString()));
			JsonArray link = rdr.readArray();
			
			List<String> ls = new LinkedList<String>();
			ls.add(link.getString(0));
			ls.add(link.getString(1));
			Collections.sort(ls);
			
			String mapkey = ls.get(0) + " - " + ls.get(1);
			
			context.write(
					new Text(mapkey), 
					new IntWritable(1));
		}
		
	}

	public static class SimpleReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for (IntWritable i : values) {
				count += i.get();
			}
			if (count == 1) {
				context.write(key, new IntWritable(0));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Simple map reduce relations");
		job.setJarByClass(SimpleMapReduceRelations.class);
		job.setMapperClass(SimpleMapper.class);
		job.setReducerClass(SimpleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
