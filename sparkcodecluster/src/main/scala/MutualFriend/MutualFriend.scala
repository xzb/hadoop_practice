package MutualFriend

/**
 * Created by xiezebin on 3/7/16.
 */

import org.apache.spark.{SparkConf, SparkContext}

object MutualFriend {

  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-Adj/soc-LiveJournal1Adj.txt"
//  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/testAdj.txt"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"

//  def mapProcess(lineOfData: String, targetUIDs: String): (String, String) = {
//    var rvCandidates = new (String, String)
//    val targetUIDPair = targetUIDs.split(",")
//
//    val userAndFriends = lineOfData.split("\\t")
//    if (userAndFriends.length < 2)
//      {
//        // no friends
//        return rvCandidates
//      }
//
//    val loUserID = userAndFriends(0)
//    if (loUserID.equals(targetUIDPair(0)) || loUserID.equals(targetUIDPair(1)))
//      {
//        rvCandidates = (targetUIDs, userAndFriends(1))
//      }
//    rvCandidates
//  }


  def reduceProcess(args:(String, Iterable[String])): String = {
    var loFriendSet = Set[String]()
    val rvMutualList = new StringBuilder()
    val loTargetPair = args._1

    args._2.foreach(line =>
      {
        val uidAndFriend = line.split("\\t")
        if (uidAndFriend.length > 1)
          {
            val friendList = uidAndFriend(1).split(",")
            friendList.foreach(friend =>
              if (loFriendSet.contains(friend))
              {
                rvMutualList.append(",")
                rvMutualList.append(friend)
              }
              else
              {
                loFriendSet += friend
              })
          }
      })

    if(rvMutualList.nonEmpty)
      {
        rvMutualList.deleteCharAt(0)
      }

    rvMutualList.insert(0, loTargetPair + "\t")
    rvMutualList.toString()
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Friend Recommendation Project")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    var loTargetUIDSet = Set[String]()
    if (args.length > 0)
      {
        args(0).split(",").foreach(uid => loTargetUIDSet += uid)
      }

    val targetBc = sc.broadcast(loTargetUIDSet)

    val logData = sc.textFile(INPUT_DIR)


//    val loMapProcessResult = logData.map(line => mapProcess(line, targetUIDs.value))
    val loMapProcessResult = logData.filter(line => targetBc.value.contains(line.split("\\t")(0)))
    val loReduceProcessResult = loMapProcessResult.groupBy(line => args(0)).map(reduceProcess)

    // need remove dir before output
    loReduceProcessResult.saveAsTextFile(OUTPUT_DIR + "test")
  }
}
