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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import by.bsuir.course.bdpa.Util;
import by.bsuir.course.bdpa.Main.TaskInfo;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

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
public class SimpleSparkSQLike implements SparkTask {
	
	public static TaskInfo TASK_INFO = new TaskInfo();
	static {
		TASK_INFO.name = "Task 3: simple sql-like by spark";
		TASK_INFO.type = "spark";
		TASK_INFO.taskClass = SimpleSparkSQLike.class;
	}
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public void doWork(JavaSparkContext ctx, String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: spark-sqlike <file> <out>");
			System.exit(1);
		}

		System.out.println("... Reading file: " + args[0]);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaPairRDD<String, String> rows = lines
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String string) {
						
						JsonReader rdr = Json.createReader(new StringReader(string));
						JsonArray row = rdr.readArray();
						
						String oid = row.getString(1);
						return new Tuple2(oid, row.toString());
					}

				});
		
		JavaPairRDD<String, Iterable<String>> grouped = rows.groupByKey();
		
		JavaPairRDD<String, String> groupedByOid = grouped
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> t) throws Exception {
						
						String oid = t._1();
						Iterable<String> rows = t._2();
						
						String order = null;
						List<String> items = new LinkedList();
						for (String row : rows) {
							JsonReader rdr = Json.createReader(new StringReader(row.toString()));
							JsonArray js = rdr.readArray();
							
							if (js.getString(0).equals("order")) {
								order = row.toString();
							} else { // line_item
								items.add(row);
							}
						}
						
						List<Tuple2<String, String>> ret = new LinkedList();						
						if (order != null) {
							for (String item : items) {
								StringBuilder resultRow = new StringBuilder();
								resultRow.append(order).append(item);
								ret.add(new Tuple2<String, String>(oid, resultRow.toString()));
							}
						}
						
						return ret;
					}
		});

		groupedByOid.saveAsTextFile(args[1] + "_" + Util.timestamp());
	}
	
}
