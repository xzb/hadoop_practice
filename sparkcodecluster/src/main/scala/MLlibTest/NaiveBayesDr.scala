package MLlibTest

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by xiezebin on 3/25/16.
 */
object NaiveBayesDr {

  val GLASS_FILE = "hdfs://cshadoop1/zxx140430/input/kmeans/glass.data"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val data = sc.textFile(GLASS_FILE)
    val parsedData = data.map { line =>
      val parts = line.split(',')
      val len = parts.length
      val label = parts(len - 1).toDouble           // last column is label in this case
      val features = parts.slice(1, parts.length - 1).map(_.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }

    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testingData = splits(1)

    val model = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")

    val labelAndPreds = testingData.map(p => (p.label, model.predict(p.features)))
    val accuracy = 1.0 * labelAndPreds.filter(x => x._1 == x._2).count() / testingData.count()

    // Save and load model
//    model.save(sc, "target/tmp/myNaiveBayesModel")
//    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }
}
