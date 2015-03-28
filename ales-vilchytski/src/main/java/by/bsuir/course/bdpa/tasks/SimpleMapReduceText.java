package by.bsuir.course.bdpa.tasks;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import by.bsuir.course.bdpa.Util;


/*
 *  Task:
 *  Given text name and example:
 *    [ "file.txt", "Foo bar baz" ]
 *    [ "another.file", "Baz foo bar" ]
 *    ...
 *  
 *  Build reverse index, which includes every word against files in which this word occurs.
 */
public class SimpleMapReduceText {

	public static class SimpleMapper extends
			Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			JsonReader rdr = Json.createReader(new StringReader(value.toString()));
			JsonArray txt = rdr.readArray();
			
			String name = txt.getString(0);
			String text = txt.getString(1);
			
			StringTokenizer tokenizer = new StringTokenizer(text);
			while(tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();
				
				if (word.matches("\\w+")) {
					context.write(
							new Text(word), 
							new Text(name));
				}
			}
		}
		
	}

	public static class SimpleReducer extends
			Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder namesList = new StringBuilder();
			for (Text name : values) {
				namesList.append(name + ", ");
			}
			
			context.write(key, new Text(namesList.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2: Simple map reduce text");
		job.setJarByClass(SimpleMapReduceText.class);
		job.setMapperClass(SimpleMapper.class);
		job.setReducerClass(SimpleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + Util.timestamp()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
