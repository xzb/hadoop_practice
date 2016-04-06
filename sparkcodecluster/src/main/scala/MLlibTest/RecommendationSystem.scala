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



/**
 * after groupby user_id, single user latent times each of rating of that user_id column
 * @param args
 * @return
 */
def oneLatentTimesRatings(args:(String, (Iterable[(String, String)], Iterable[DenseVector[Double]])))
: Array[(String, (DenseMatrix[Double], DenseVector[Double]))] = {
  val userId = args._1
  val userLatent = args._2._2.head
  val ratingVector = args._2._1

  var rv = new ArrayBuffer[(String, (DenseMatrix[Double], DenseVector[Double]))]()

  ratingVector.foreach(itemIdRatePair => {
    val itemId = itemIdRatePair._1
    val singleRate = itemIdRatePair._2.toDouble

    val timesResult = singleRate * userLatent

    rv += ((itemId, (userLatent * userLatent.t, timesResult)))
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

val regMatrixBC = sc.broadcast(regMatrix)


for( i <- 1 to 10){

  //===========================================Homework 4. Implement code to calculate equation 2 and 3 .===================================================
  //=================You will be required to write code to update the myuserMatrix which contains the latent vectors for each user and myitemMatrix which is the matrix that contains the latent vector for the items
  //Please Fill in your code here.

  // *** update item latent ***
  myitemMatrix =
    ratingByUser.value.cogroup(myuserMatrix).
      flatMap(oneLatentTimesRatings).
      reduceByKey((res1, res2) => {
        ((res1._1 + res2._1), (res1._2 + res2._2))
      }).
      map(tuple => {
        val itemId = tuple._1
        val sumMatrix = inv(tuple._2._1 + regMatrixBC.value)
        val sumVector = tuple._2._2
        (itemId, sumMatrix * sumVector)
      }).
      persist

  // *** update user latent ***
  myuserMatrix =
    ratingByItem.value.cogroup(myitemMatrix).
      flatMap(oneLatentTimesRatings).
      reduceByKey((res1, res2) => {
        ((res1._1 + res2._1), (res1._2 + res2._2))
      }).
      map(tuple => {
        val userId = tuple._1
        val sumMatrix = inv(tuple._2._1 + regMatrixBC.value)
        val sumVector = tuple._2._2
        (userId, sumMatrix * sumVector)
      }).
      persist

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


