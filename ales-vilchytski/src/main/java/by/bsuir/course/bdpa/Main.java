package by.bsuir.course.bdpa;

import java.util.Arrays;

import by.bsuir.course.bdpa.examples.WordCountHadoop;
import by.bsuir.course.bdpa.examples.WordCountSpark;


/**
 * Classes in this package are written just for simplifying local runs and debug of tasks.
 */
public class Main {
	
	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			throw new IllegalArgumentException("Wrong args. Use following arguments: '[-d|--debug] <taskType>-<taskName> <args...>'");
		}
		String task = args[0];
		String[] arguments =  Arrays.copyOfRange(args, 1, args.length);
		
		int ret = -1;
		if (task.startsWith("hadoop-")) {
			if (task.contains("wordcount")) {
				WordCountHadoop.main(arguments);
			} else {
				ret = new HadoopTasksRunner().run(task.substring("hadoop-".length()), arguments);
			}
		} else if (task.startsWith("spark-")) {
			if (task.contains("wordcount")) {
				WordCountSpark.main(arguments);
			} else {
				ret = new SparkTasksRunner().run(task.substring("spark-".length()), arguments);
			}
		} else {
			throw new IllegalArgumentException("Unknown task type, use smth like 'hadoop-' or 'spark-'");
		}
		
		System.exit(ret);
	}
	
}
