/**
 * Created by xiezebin on 3/7/16.
 */

import org.apache.spark.{SparkContext, SparkConf}
import MutualFriend.reduceProcess

object MutualFriendZip {

  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-Adj/soc-LiveJournal1Adj.txt"
  val INPUT_INFO_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-userdata"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"

  def mapProcessZip(value:String, mutualSet:Set[String]): String = {
    // case 1: no mutual friend, just return    todo emit one empty record
    if (mutualSet.isEmpty)
    {
      return ""
    }

    val loLineOfData = value.split(",")
    val loCurrentUID = loLineOfData(0)
    // case 2: current user is not mutual friend, just return
    if (!mutualSet.contains(loCurrentUID))
    {
      return ""
    }

    // emit data
    val loFriendName = loLineOfData(1) + " " + loLineOfData(2)
    val loValueOut = loLineOfData(6)              // zip
    loFriendName + ":" + loValueOut
  }

  def reduceProcessZip(args:(String, Iterable[String])): String = {
    val loBuilder = new StringBuilder()
    args._2.foreach(lpMutualZip =>
    {
      loBuilder.append(",")
      loBuilder.append(lpMutualZip)
    })
    if (loBuilder.nonEmpty)
    {
      loBuilder.deleteCharAt(0)
    }

    args._1 + "\t" + loBuilder.toString()
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
    var targetBc = sc.broadcast(loTargetUIDSet)

    val logData = sc.textFile(INPUT_DIR)
    val loMapStepOneResult = logData.filter(line => targetBc.value.contains(line.split("\\t")(0)))
    val loReduceStepOneResult = loMapStepOneResult.groupBy(line => args(0)).map(reduceProcess)


    // create mutual set for step two
    val loMutualArray = loReduceStepOneResult.collect()(0).split("\\t")(1).split(",")
    var loMutualSet = Set[String]()
    loMutualArray.foreach(friend => loMutualSet += friend)
    targetBc = sc.broadcast(loMutualSet)


    val loUserData = sc.textFile(INPUT_INFO_DIR)
    val loFilterResult = loUserData.filter(line => targetBc.value.contains(line.split(",")(0)))
    val loFindZipResult = loFilterResult.map(line => mapProcessZip(line, targetBc.value))
    val loShuffleResult = loFindZipResult.groupBy(line => args(0))
    val loFinalResult = loShuffleResult.map(reduceProcessZip)

    loFinalResult.saveAsTextFile(OUTPUT_DIR + "test")
  }
}
