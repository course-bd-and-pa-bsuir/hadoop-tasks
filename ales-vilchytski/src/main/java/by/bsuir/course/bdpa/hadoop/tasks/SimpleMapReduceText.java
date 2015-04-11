package by.bsuir.course.bdpa.hadoop.tasks;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import by.bsuir.course.bdpa.HadoopTasksRunner.TaskInfo;

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

	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 2: simple mapreduce text reverse index";
		TASK_INFO.mapper = SimpleMapper.class;
		TASK_INFO.reducer = SimpleReducer.class;
	}
	
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

}
