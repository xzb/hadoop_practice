package MLlibTest

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.ArrayBuffer

/**
 * Created by xiezebin on 3/23/16.
 */
object KMeansDr {

  val RATING_FILE = "hdfs://cshadoop1/zxx140430/input/kmeans/ratings.dat"
  val USER_FILE = "hdfs://cshadoop1/zxx140430/input/kmeans/users.dat"
  val MOVIE_FILE = "hdfs://cshadoop1/zxx140430/input/kmeans/movies.dat"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val userRaw = sc.textFile(USER_FILE)
    val userSizeBc = sc.broadcast(userRaw.count().toInt)
    val ratingRaw = sc.textFile(RATING_FILE)
    // userID::MovieID::Rating::TimeStamp

    /*
          uid1  uid2  uid3
    mov1   3     4     0        --> each line is a Vector
    mov2   1     2     5
    mov3   2     4     0
    */
    val movieToRatings = ratingRaw.map(line => {
      val info = line.split("::")
      (info(1), line)             // key is movieID
    }).
    groupByKey()                 // each movie has a group of ratings by user

    val ratingVectors = movieToRatings.map(args => {
      val seqBuffer = new ArrayBuffer[(Int, Double)]()
      args._2.foreach(line => {   // create sparse Vectors
        val info = line.split("::")
        val uid = info(0)
        val index = uid.toInt - 1
        val rating = info(2).toDouble
        seqBuffer += ((index, rating))
      })

      Vectors.sparse(userSizeBc.value, seqBuffer.toSeq)
    }).
    cache()


    //**** Cluster the data into two classes using KMeans ****
    val numClusters = 10
    val numIterations = 20
    val clustersModel = KMeans.train(ratingVectors, numClusters, numIterations)


    //**** find 5 movies id for each cluster ****
    var clusterSamples = new Array[ArrayBuffer[String]](numClusters)
    for(i <- 0 until numClusters)
    {
      // initialize array
      clusterSamples(i) = new ArrayBuffer[String]()
    }

    val movieIDs = movieToRatings.keys.collect()
    val predictRes = clustersModel.predict(ratingVectors).collect()
    val samplePerClus = 5
    var movieIDSet = Set[String]()
    for (i <- 0 until movieIDs.length)
    {
      val clus = predictRes(i)
      if (clusterSamples(clus).length < samplePerClus)    // some cluster don't have 5 sample
      {
        clusterSamples(clus) += movieIDs(i)
        movieIDSet += movieIDs(i)           // save the movie id
      }
    }


    //**** join the movie information ****
    val movieIDSetBc = sc.broadcast(movieIDSet)
    val movieRaw = sc.textFile(MOVIE_FILE)
    // movieID::Title::Genres

    var selectedMoviesMap = Map[String, String]()
    val selectedMovies = movieRaw.filter(line => {
      val info = line.split("::")
      val mid = info(0)
      movieIDSetBc.value.contains(mid)
    }).collect()
    selectedMovies.foreach(line => {
        val mid = line.split("::")(0)
        selectedMoviesMap += (mid -> line.replaceAll("::", ","))
      })

    
    //**** print results ****
    for (i <- 0 until clusterSamples.length)
    {
      println("Cluster: " + i)
      clusterSamples(i).foreach(mid => {
        println(selectedMoviesMap(mid))
      })
      println()
    }



    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clustersModel.computeCost(ratingVectors)
//    println("Within Set Sum of Squared Errors = " + WSSSE)



    // test case
    /*var ratings = new ArrayBuffer[String]()
    ratings += "0 4 2 1 5"
    ratings += "4 2 2 0 1"
    ratings += "0 0 5 1 3"
    ratings += "0 4 2 1 4"
    val ratingsRdd = sc.parallelize(ratings.toList)
    val parsed = ratingsRdd.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    clusters.predict(Vectors.dense("0 4 2 1 3".split(' ').map(_.toDouble)))
    */
  }
}
