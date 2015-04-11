package by.bsuir.course.bdpa;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import by.bsuir.course.bdpa.spark.tasks.SimpleSparkRelations;
import by.bsuir.course.bdpa.spark.tasks.SimpleSparkSQLike;
import by.bsuir.course.bdpa.spark.tasks.SimpleSparkText;
import by.bsuir.course.bdpa.spark.tasks.SparkTask;

public class SparkTasksRunner {
	public static class TaskInfo {
		public String name;
		public Class<? extends SparkTask> taskClass;
	}
	
	private final Map<String, TaskInfo> TASKS_MAP = new HashMap<String, TaskInfo>();
	{
		TASKS_MAP.put("relations", SimpleSparkRelations.TASK_INFO);
		TASKS_MAP.put("sqlike", SimpleSparkSQLike.TASK_INFO);
		TASKS_MAP.put("text", SimpleSparkText.TASK_INFO);
	}
	
	public int run(String task, String[] args) throws Exception {
		TaskInfo info = TASKS_MAP.get(task);
		
		if (info == null) {
			throw new IllegalArgumentException(String.format("Specified task %s is not supported. Supported ones: %s", task, TASKS_MAP.keySet().toString()));
		}
		
		SparkTask sparkTask = info.taskClass.getConstructor().newInstance();

		SparkConf sparkConf = new SparkConf().setAppName(info.name);
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		try {
			sparkTask.doWork(ctx, args);
		} finally {
			ctx.stop();
		}
		
		return 0;
	}
	
}
