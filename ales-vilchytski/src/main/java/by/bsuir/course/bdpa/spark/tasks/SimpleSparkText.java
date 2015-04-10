/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.bsuir.course.bdpa.spark.tasks;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import by.bsuir.course.bdpa.Util;
import by.bsuir.course.bdpa.Main.TaskInfo;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

/*
 *  Task:
 *  Given text name and example:
 *    [ "file.txt", "Foo bar baz" ]
 *    [ "another.file", "Baz foo bar" ]
 *    ...
 *  
 *  Build reverse index, which includes every word against files in which this word occurs.
 */
public class SimpleSparkText implements SparkTask {
	
	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 2: simple text by spark";
		TASK_INFO.type = "spark";
		TASK_INFO.taskClass = SimpleSparkText.class;
	}
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public void doWork(JavaSparkContext ctx, String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: spark-text <file> <out>");
			System.exit(1);
		}

		System.out.println("... Reading file: " + args[0]);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaPairRDD<String, String> pairs = lines
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
					public Iterable<Tuple2<String, String>> call(String string) {
						
						JsonReader rdr = Json.createReader(new StringReader(string));
						JsonArray txt = rdr.readArray();
						
						String name = txt.getString(0);
						String text = txt.getString(1);
						
						List<Tuple2<String, String>> tuples = new LinkedList<Tuple2<String,String>>();
						
						StringTokenizer tokenizer = new StringTokenizer(text);
						while(tokenizer.hasMoreTokens()) {
							String word = tokenizer.nextToken().toLowerCase();
							
							if (word.matches("\\w+")) {
								tuples.add(new Tuple2<String, String>(word, name));
							}
						}
						return tuples;
					}
				});
		

		JavaPairRDD<String, String> index = pairs
				.reduceByKey(new Function2<String, String, String>() {
					public String call(String name1, String name2) {
						return name1 + ", " + name2;
					}
				});

		index.saveAsTextFile(args[1] + "_" + Util.timestamp());
	}
	
}
