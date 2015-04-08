package by.bsuir.course.bdpa;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import by.bsuir.course.bdpa.spark.tasks.SimpleSparkRelations;
import by.bsuir.course.bdpa.spark.tasks.SimpleSparkSQLike;
import by.bsuir.course.bdpa.spark.tasks.SimpleSparkText;
import by.bsuir.course.bdpa.spark.tasks.SparkTask;
import by.bsuir.course.bdpa.tasks.SimpleMapReduceRelations;
import by.bsuir.course.bdpa.tasks.SimpleMapReduceSqlike;
import by.bsuir.course.bdpa.tasks.SimpleMapReduceText;

public class Main {

	public static class TaskInfo {
		public String name;
		public String type = "hadoop";
		
		// Spark settings
		public Class<?> taskClass;
		
		// Hadoop settings
		public Class<? extends Mapper> mapper;
		public Class<? extends Reducer> reducer;
		public Class<?> mapKey = Text.class;
		public Class<?> mapValue = Text.class;
		public Class<?> outKey = Text.class;
		public Class<?> outValue = Text.class;
	}
	
	private final static Map<String, TaskInfo> TASKS_MAP = new HashMap<String, TaskInfo>();
	static {
		TASKS_MAP.put("relations", SimpleMapReduceRelations.TASK_INFO);
		TASKS_MAP.put("sqlike", SimpleMapReduceSqlike.TASK_INFO);
		TASKS_MAP.put("text", SimpleMapReduceText.TASK_INFO);
		TASKS_MAP.put("spark-relations", SimpleSparkRelations.TASK_INFO);
		TASKS_MAP.put("spark-sqlike", SimpleSparkSQLike.TASK_INFO);
		TASKS_MAP.put("spark-text", SimpleSparkText.TASK_INFO);
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if (args.length < 2) {
			throw new IllegalArgumentException("Wrong args. Use following arguments: '[-d|--debug] <taskName> <args...>'");
		}
		String task = args[0];
		TaskInfo info = TASKS_MAP.get(task);
		
		if (info == null) {
			throw new IllegalArgumentException(String.format("Specified task %s is not supported. Supported ones: %s", task, TASKS_MAP.keySet().toString()));
		}
		
		if (info.type.equals("hadoop")) {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, info.name);
			job.setJarByClass(Main.class);
			job.setMapperClass(info.mapper);
			job.setReducerClass(info.reducer);
			job.setMapOutputKeyClass(info.mapKey);
			job.setMapOutputValueClass(info.mapValue);
			job.setOutputKeyClass(info.outKey);
			job.setOutputValueClass(info.outValue);
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2] + "_" + Util.timestamp()));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else if (info.type.equals("spark")){
			SparkTask sparkTask = (SparkTask) info.taskClass.getConstructor().newInstance();

			SparkConf sparkConf = new SparkConf().setAppName(info.name);
			JavaSparkContext ctx = new JavaSparkContext(sparkConf);
			
			sparkTask.doWork(ctx, Arrays.copyOfRange(args, 1, 100500));
			
			ctx.stop();
		} else {
			throw new IllegalArgumentException("Unknown task type");
		}
	}
	
}
