package by.bsuir.course.bdpa.tasks;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import by.bsuir.course.bdpa.Main.TaskInfo;


/*
 *  Task:
 *  Given SQL-like query with inner join:
 *  SELECT * FROM A, B WHERE A.id = B.id
 *  
 *  MapReduce should produce the same output as this query, but using following input:
 *    ["A", "1", "foo", "bar"]  
 *    ["B", "1", "baz", "foo"]
 *    ...
 */
public class SimpleMapReduceSqlike {

	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 3: simple mapreduce sqlike join";
		TASK_INFO.mapper = SimpleMapper.class;
		TASK_INFO.reducer = SimpleReducer.class;
	}
	
	public static class SimpleMapper extends
			Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			JsonReader rdr = Json.createReader(new StringReader(value.toString()));
			JsonArray row = rdr.readArray();
			
			String oid = row.getString(1);
			
			context.write(new Text(oid), new Text(row.toString()));
		}
		
	}

	public static class SimpleReducer extends
			Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String order = null;
			List<String> items = new LinkedList<String>();
			for (Text row : values) {
				JsonReader rdr = Json.createReader(new StringReader(row.toString()));
				JsonArray js = rdr.readArray();
				
				if (js.getString(0).equals("order")) {
					order = row.toString();
				} else { // line_item
					items.add(row.toString());
				}
			}
			
			if (order != null) {
				for (String item : items) {
					StringBuilder resultRow = new StringBuilder();
					resultRow.append(order).append(item);
					context.write(key, new Text(resultRow.toString()));
				}
			}
			
		}
	}
	
}
