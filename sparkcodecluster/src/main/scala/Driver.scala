package edu.utdallas.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties

object Driver {
  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Friend Recommendation Project")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val logFile = INPUT_DIR + "testinput.txt" // Should be some file on your system
    val logData = sc.textFile(logFile).cache()

    val counts = logData.flatMap(line => line.split(" "))
                  .map(word => (word, 1))
                  .reduceByKey(_ + _)

    // need remove dir before output
    counts.saveAsTextFile(OUTPUT_DIR + "test")
  }
}

