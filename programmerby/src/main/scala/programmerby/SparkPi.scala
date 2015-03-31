package programmerby

import scala.math.random

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Computes an approximation to pi
 *
 * Source: https://github.com/granthenke/spark-demo/tree/master/src/main/scala/com/cloudera/sa
 */
object SparkPi {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: <master> [<slices>]")
      System.exit(1)
    }

    // Process Args
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)
    val slices = if (args.length > 1) args(1).toInt else 2
    val n = 100000 * slices

    // Run spark job
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

    // Output & Close
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
