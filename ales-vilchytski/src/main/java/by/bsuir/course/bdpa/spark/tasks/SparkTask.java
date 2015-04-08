package by.bsuir.course.bdpa.spark.tasks;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkTask extends Serializable {

	public void doWork(JavaSparkContext ctx, String[] args);
	
}
