/**
 * Created by xiezebin on 3/7/16.
 */

package edu.utdallas.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

object RecommendFriend {
  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-Adj/soc-LiveJournal1Adj.txt"
//  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/testAdj.txt"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"


  def processEachFriendList(args: String, targetUIDs: String): Array[((String, String), Int)] =
  {
    // input "5 \t 2,3,4" -- output tuple ((2,3),1) ((2,4),1) etc
    val loUserAndFriend = args.split("\\t")
    // case 1: have no friend
    if (loUserAndFriend.length < 2)
      {
        return new Array[((String, String), Int)](0)        // should not return null
      }

    val loCurrentUID = loUserAndFriend(0)
    val loFriendList = loUserAndFriend(1).split(",")

    // handle direct friend here

    // case 2: only zero or one friend
    if (loFriendList.length < 2)
      {
        return new Array[((String, String), Int)](0)
      }

    var rvMutualFriend = new ArrayBuffer[((String, String), Int)]()
    for(i <- 0 until loFriendList.length)
      {
        for(j <- 0 until loFriendList.length)
          {
            if (i != j)
              {
                val item = ((loFriendList(i), loFriendList(j)) , 1)
                rvMutualFriend += item
              }
          }
      }
    rvMutualFriend.toArray
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Friend Recommendation Project")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    var loFirstArgu = ""
    if (args.length > 0)
      {
        loFirstArgu = args(0)
      }
    val targetUIDs = sc.broadcast(loFirstArgu)

    val logFile = INPUT_DIR
    val logData = sc.textFile(logFile)

    val loProcessResult = logData.flatMap(line => processEachFriendList(line, targetUIDs.value))
//    val loProcessResult = logData.flatMap(x => x.split(",")).map(x => (x, 1))

    val loCountMutual = loProcessResult.reduceByKey(_ + _)

    val rvData = loCountMutual


//    val loList = loFriendAndList.collect()._1
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)


    // need remove dir before output
    rvData.saveAsTextFile(OUTPUT_DIR + "test")
  }
}

