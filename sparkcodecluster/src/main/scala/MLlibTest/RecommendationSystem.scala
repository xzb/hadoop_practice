/**
 * Created by xiezebin on 3/27/16.
 */
import org.apache.spark._
val conf = new SparkConf().setAppName("")
conf.set("spark.hadoop.validateOutputSpecs", "false")
val sc = new SparkContext(conf)
// comment above when running in shell


import scala.collection.mutable.ArrayBuffer
import breeze.linalg._
import org.apache.spark.HashPartitioner
//spark-shell -i als.scala to run this code
//SPARK_SUBMIT_OPTS="-XX:MaxPermSize=4g" spark-shell -i als.scala


//Implementation of sec 14.3 Distributed Alternating least squares from stanford Distributed Algorithms and Optimization tutorial.

//loads ratings from file
val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu//hw3spring/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2)))

// counts unique movies
val itemCount = ratings.map(x=>x._2).distinct.count

// counts unique user
val userCount = ratings.map(x=>x._1).distinct.count

// get distinct movies
val items = ratings.map(x=>x._2).distinct

// get distinct user
val users = ratings.map(x=>x._1).distinct

// latent factor
val k= 5

//create item latent vectors
val itemMatrix = items.map(x=> (x,DenseVector.zeros[Double](k)))
//Initialize the values to 0.5
// generated a latent vector for each item using movie id as key Array((movie_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myitemMatrix = itemMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist

//create user latent vectors
val userMatrix = users.map(x=> (x,DenseVector.zeros[Double](k)))
//Initialize the values to 0.5
// generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myuserMatrix = userMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist

// group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1,x._3))))

// group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5))
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3))))



def oneLatentTimesRatings(args:(String, (Iterable[(String, String)], Iterable[DenseVector[Double]])))
: Array[(String, DenseVector[Double])] = {
  val userId = args._1
  val userLatent = args._2._2.head
  val ratingVector = args._2._1

  var rv = new ArrayBuffer[(String, DenseVector[Double])]()

  ratingVector.foreach(itemIdRatePair => {
    val itemId = itemIdRatePair._1
    val singleRate = itemIdRatePair._2.toDouble

    val timesResult = singleRate * userLatent

    rv += ((itemId, timesResult))
  })

  rv.toArray
}

/**
 * reduce by adding the value of same item id
 */
def reduceLatents(arg1:(Array[(String, DenseVector[Double])]),
                  arg2:(Array[(String, DenseVector[Double])]))
: Array[(String, DenseVector[Double])] = {

  var rv = new ArrayBuffer[(String, DenseVector[Double])]()

  var itemIdMap = Map[String, DenseVector[Double]]()
  arg2.foreach(tuple => {
    itemIdMap += (tuple._1 -> tuple._2)
  })

  arg1.foreach(tuple => {
    val itemId = tuple._1
    val vector = tuple._2
    if (itemIdMap.contains(itemId)) {
      itemIdMap += (itemId -> (vector + itemIdMap(itemId)))
    }
    else {
      itemIdMap += (tuple._1 -> tuple._2)
    }
  })

  itemIdMap.foreach(tuple => {
    val itemId = tuple._1
    val vector = tuple._2
    rv += ((itemId, vector))
  })
  rv.toArray
}


// regularization factor which is lambda.
val regfactor = 1.0
val regMatrix = DenseMatrix.zeros[Double](k,k)  //generate an diagonal matrix with dimension k by k
//filling in the diagonal values for the reqularization matrix.
regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t
regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t
regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t
regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t
regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t



for( i <- 1 to 10){

  //===========================================Homework 4. Implement code to calculate equation 2 and 3 .===================================================
  //=================You will be required to write code to update the myuserMatrix which contains the latent vectors for each user and myitemMatrix which is the matrix that contains the latent vector for the items
  //Please Fill in your code here.

  // *** update item latent ***
  // 1. calculate (sum_i(user_i * user_i^T) + I) ^ -1
  val userDiag = inv(
    myuserMatrix.map(userLatentPair => {
      val userLatent = userLatentPair._2
      userLatent * userLatent.t
    }).
      reduce(_ + _) + regfactor * regMatrix
  )
  val userDiagBC = sc.broadcast(userDiag)

  val myitemMatrixLocal =
    ratingByUser.value.cogroup(myuserMatrix).
      map(oneLatentTimesRatings).
      reduce(reduceLatents).
      map(tuple => {
        (tuple._1, userDiagBC.value * tuple._2)
      })
  myitemMatrix = sc.parallelize(myitemMatrixLocal).persist

  // *** update user latent ***
  // 2. calculate (sum_i(item_i * item_i^T) + I) ^ -1
  val itemDiag = inv(
    myitemMatrix.map(itemLatentPair => {
      val itemLatent = itemLatentPair._2
      itemLatent * itemLatent.t
    }).
      reduce(_ + _) + regfactor * regMatrix
  )
  val itemDiagBC = sc.broadcast(itemDiag)

  val myuserMatrixLocal =
    ratingByItem.value.cogroup(myitemMatrix).
      map(oneLatentTimesRatings).
      reduce(reduceLatents).
      map(tuple => {
        (tuple._1, itemDiagBC.value * tuple._2)
      })
  myuserMatrix = sc.parallelize(myuserMatrixLocal).persist

}
//==========================================End of update latent factors=================================================================

//======================================================Implement code to recalculate the ratings a user will give an item.====================

//Hint: This requires multiplying the latent vector of the user with the latent vector of the  item. Please take the input from the command line. and
// Provide the predicted rating for user 1 and item 914, user 1757 and item 1777, user 1759 and item 231.

//Your prediction code here

def printUserLatent(userId:String) : Unit =
{
  println(myuserMatrix.filter(tuple => tuple._1.equals(userId)).first()._2)
}
def printItemLatent(movieId:String): Unit =
{
  println(myitemMatrix.filter(tuple => tuple._1.equals(movieId)).first()._2)
}

def predictRate(userId:String, movieId:String): Unit =
{
  val userLatent = myuserMatrix.filter(tuple => tuple._1.equals(userId)).first()._2
  val itemLatent = myitemMatrix.filter(tuple => tuple._1.equals(movieId)).first()._2
  val rate = userLatent.t * itemLatent
  println(rate)
}


