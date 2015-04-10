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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import by.bsuir.course.bdpa.Main.TaskInfo;
import by.bsuir.course.bdpa.Util;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

/*
 *  Task:
 *  Given json-like file in format 
 *    [ "A", "B" ]
 *    [ "C", "A" ]
 *    ...
 *  
 *  Find out all pairs, which has relations like 'A -> B' or 'B -> A' but not both.
 */
public class SimpleSparkRelations implements SparkTask {
	
	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 1: simple relations by spark";
		TASK_INFO.type = "spark";
		TASK_INFO.taskClass = SimpleSparkRelations.class;
	}
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public void doWork(JavaSparkContext ctx, String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: spark-relations <file> <out>");
			System.exit(1);
		}

		System.out.println("... Reading file: " + args[0]);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaRDD<String> links = lines
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String string) {
						
						JsonReader rdr = Json.createReader(new StringReader(string.toString()));
						JsonArray link = rdr.readArray();
						
						List<String> ls = new LinkedList<String>();
						ls.add(link.getString(0));
						ls.add(link.getString(1));
						Collections.sort(ls);
						
						String mapkey = ls.get(0) + " - " + ls.get(1);
						
						return Arrays.asList(mapkey);
					}
				});

		JavaPairRDD<String, Integer> ones = links
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		
		JavaPairRDD<String, Integer> filtered = counts.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> row) throws Exception {
				return row._2().intValue() < 2;
			}
		});

		filtered.saveAsTextFile(args[1] + "_" + Util.timestamp());
	}
	
}
