package MutualFriend

/**
 * Created by xiezebin on 3/7/16.
 */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TopAverageAge {

  val INPUT_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-Adj/soc-LiveJournal1Adj.txt"
  val INPUT_INFO_DIR = "hdfs://cshadoop1/zxx140430/input/hw1-userdata"
  val OUTPUT_DIR = "hdfs://cshadoop1/zxx140430/output/"

  val CURRENT_YEAR = "2016"
  val CURRENT_MONTH = "2"

  // STEP 1: Join two tables by key of each friend in list
  def JoinFriendMapper(value: String): Array[(String, String)] = {
    var rvTupleArray = new ArrayBuffer[(String, String)]()
    val loLineOfData = value.split("\\t")
    if (loLineOfData.length < 2)
    {
      return rvTupleArray.toArray
    }

    val loCurrentUID = loLineOfData(0)
    val loFriend = loLineOfData(1).split(",")
    loFriend.foreach(friend =>
    {
      rvTupleArray += ((friend, loCurrentUID))
    })

    rvTupleArray.toArray
  }

  def JoinAgeMapper(value:String): (String, Double) = {

    val loLineOfData = value.split(",")

    val loCurrentUID = loLineOfData(0)
    val loDateOfBirth = loLineOfData(9)
    val loAge = Integer.valueOf(CURRENT_YEAR) - Integer.valueOf(loDateOfBirth.split("/")(2))
    + (Integer.valueOf(CURRENT_MONTH) - Integer.valueOf(loDateOfBirth.split("/")(0))) * 1.0 / 12

    (loCurrentUID, loAge)
  }

  /**
   * @param args (uid, (FriendList, Age))
   * @return (each friend, Age)
   */
  def JoinFriendAgeReducer(args:(String, (Iterable[String], Iterable[Double]))): Array[(String, Double)] = {
    var rvTupleArray = new ArrayBuffer[(String, Double)]()

    // reduce-side 1-many join, first find age
//    val loAge = args._3.iterator.next()
    val loAge = args._2._2.head

    val loFriendItr = args._2._1
    loFriendItr.foreach(value =>
    {
      rvTupleArray += ((value, loAge))
    })
    rvTupleArray.toArray
  }


  // STEP 2: Calculate average age of friends of each user
  /**
   * @param args (uid, Ages from friends)
   * @return (ave age of friends, uid)
   */
  def CalculateAveAgeReducer(args:(String, Iterable[Double])): (Double, String) = {
    var loAgeSum = 0.0
    var loCount = 0
    val values = args._2
    values.foreach(loContent =>                 //todo no friend
    {
      loAgeSum += loContent
      loCount += 1
    })

    if (loCount == 0)
    {
      return (0, args._1)
    }
    val loAveAge = loAgeSum * 1.0 / loCount
//    val loOutput = "%.2f".format(loAveAge)
    (loAveAge, args._1)
  }


  // STEP 3: Sort by average age

  // STEP 4: Join Address
  def JoinAddressMapper(value:String, targetMap: Map[String, Double]): String = {

    val loLineOfData = value.split(",")

    val loCurrentUID = loLineOfData(0)
    val loAddress = loLineOfData(3) + ", " + loLineOfData(4) + ", " + loLineOfData(5)

    val loAveAgeOfFriend = "%.2f".format(targetMap.get(loCurrentUID).get)

    loCurrentUID + ", " + loAddress + ", " + loAveAgeOfFriend
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Friend Recommendation Project")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val loAdjData = sc.textFile(INPUT_DIR)
    val loInfoData = sc.textFile(INPUT_INFO_DIR)

    // Step one:
    val loFriendMapperRes = loAdjData.flatMap(JoinFriendMapper)
    val loAgeMapperRes = loInfoData.map(JoinAgeMapper)
    val loJoinFriendAgeRes = loFriendMapperRes.cogroup(loAgeMapperRes).flatMap(JoinFriendAgeReducer)

    // Step two:
    val loCalculateAveRes = loJoinFriendAgeRes.groupByKey().map(CalculateAveAgeReducer)

    // Step three:
    val loSortByAgeDescRes = loCalculateAveRes.sortByKey(false).map(tuple => (tuple._2, tuple._1))    // revert to (uid, ave age)

    // Step four:
    val loTop20AveRes = loSortByAgeDescRes.take(20)
    var loTargetMap = Map[String, Double]()
    loTop20AveRes.foreach(tuple => loTargetMap += (tuple._1 -> tuple._2))
    val targetBc = sc.broadcast(loTargetMap)
    val loAddressMapperRes = loInfoData.filter(line => targetBc.value.contains(line.split(",")(0))).
      map(line => JoinAddressMapper(line, targetBc.value))


    val loFinalRes = loAddressMapperRes
    loFinalRes.saveAsTextFile(OUTPUT_DIR + "test")
  }
}
