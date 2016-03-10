/**
 * Created by xiezebin on 3/7/16.
 */

package edu.utdallas.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


import scala.collection.mutable.ArrayBuffer

object RecommendFriend {
  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-Adj/soc-LiveJournal1Adj.txt"
//  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/testAdj.txt"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"

  val IS_FRIEND = -1

  def mapProcess(args: String, targetUIDs: String): Array[(String, (String, Int))] =
  {
    // input "5 \t 2,3,4" -- output tuple (2,(3,1)) (2,(4,1)) etc
    var rvMutualFriend = new ArrayBuffer[(String, (String, Int))]()

    val loUserAndFriend = args.split("\\t")
    // case 0: have no friend
    if (loUserAndFriend.length < 2)
    {
      return rvMutualFriend.toArray            // should not return null
    }

    val loCurrentUID = loUserAndFriend(0)
    val loFriendList = loUserAndFriend(1).split(",")

    var loTargetUIDSet = Set[String]()
    targetUIDs.split(",").foreach(uid => loTargetUIDSet += uid)


    // case 1: target user is current user, set direct friend
    if (loTargetUIDSet.contains(loCurrentUID))
    {
      for (i <- 0 until loFriendList.length)
      {
        rvMutualFriend += ((loCurrentUID, (loFriendList(i), IS_FRIEND)))
      }
    }

    // check whether target users are in the friend list
    var loCurrentTargetUIDSet = Set[String]()
    for (i <- 0 until loFriendList.length)
    {
      if (loTargetUIDSet.contains(loFriendList(i)))
      {
        loCurrentTargetUIDSet += loFriendList(i)
      }
    }
    // case 2: friend list don't have target user, just discard
    if (loCurrentTargetUIDSet.isEmpty)
    {
      return rvMutualFriend.toArray
    }

    // case 3: target user is in friend list, emit target user and his candidate friend, mutual friend is current user
    loCurrentTargetUIDSet.foreach(lpTargetUID =>
    {
      for(j <- 0 until loFriendList.length)
      {
        if (!lpTargetUID.equals(loFriendList(j)))
        {
          val item = (lpTargetUID, (loFriendList(j), 1))
          rvMutualFriend += item
        }
      }
    }
    )
    rvMutualFriend.toArray
  }

  def reduceProcess(args:(String, Iterable[(String, Int)])): String = {
    val loKey = args._1
    var loUnsortedCandidates = Map[String, Int]()

    args._2.foreach(lpCandidateFriend =>
    {
      val loCandidate = lpCandidateFriend._1
      val loMutualFriend = lpCandidateFriend._2

      if (loUnsortedCandidates.contains(loCandidate))
      {
        val loCount = loUnsortedCandidates(loCandidate)
        if (loCount == -1)
        {
          // ignore direct friend

        }
        else if (loMutualFriend.equals(IS_FRIEND))
        {
          // reset direct friend
          loUnsortedCandidates += (loCandidate -> -1)
        }
        else
        {
          loUnsortedCandidates += (loCandidate -> (loCount + 1))
        }
      }
      else
      {
        if (loMutualFriend.equals(IS_FRIEND))
        {
          loUnsortedCandidates += (loCandidate -> -1)
        }
        else
        {
          loUnsortedCandidates += (loCandidate -> 1)
        }
      }
    })

    // sort the candidates by the count of mutual friends with target user (no need)
    // output top 10 recommended friends (no need)

    var loBuilder = new StringBuilder()
    loUnsortedCandidates.foreach(entry =>
    {
      // direct friend
      if (entry._2 != -1) {
        loBuilder.append(",")
        loBuilder.append(entry._1)
      }
    })

    if (loBuilder.nonEmpty)
    {
      loBuilder.deleteCharAt(0)
    }

    loBuilder.insert(0, loKey + "\t")
    loBuilder.toString()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Friend Recommendation Project")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    /*var loFirstArgu = ""
    if (args.length > 0)
      {
        loFirstArgu = args(0)
      }
    val targetUIDs = sc.broadcast(loFirstArgu)*/
    val targetUIDs = sc.broadcast("924,8941,8942,9019,9020,9021,9022,9990,9992,9993")

    val logData = sc.textFile(INPUT_DIR)

    //    val loProcessResult = logData.flatMap(x => x.split(",")).map(x => (x, 1))
    val loMapProcessResult = logData.flatMap(line => mapProcess(line, targetUIDs.value))
    val loShuffleResult = loMapProcessResult.groupByKey()
    val loReduceProcessResult = loShuffleResult.map(reduceProcess)

    val loCountMutual = loReduceProcessResult//.reduceByKey(_ + _)

    val rvData = loCountMutual


    //    val loList = loFriendAndList.collect()._1
    //      .map(word => (word, 1))
    //      .reduceByKey(_ + _)


    // need remove dir before output
    rvData.saveAsTextFile(OUTPUT_DIR + "test")
  }
}

