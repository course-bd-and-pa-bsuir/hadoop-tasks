package by.bsuir.course.bdpa.hadoop.tasks;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import by.bsuir.course.bdpa.HadoopTasksRunner.TaskInfo;


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
	
	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 1: simple mapreduce relations";
		TASK_INFO.mapper = SimpleMapper.class;
		TASK_INFO.reducer = SimpleReducer.class;
		TASK_INFO.mapValue = IntWritable.class;
		TASK_INFO.outValue = IntWritable.class;
	}
	
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

}
