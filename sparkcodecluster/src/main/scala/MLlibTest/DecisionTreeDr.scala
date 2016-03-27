package MLlibTest

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree

/**
 * Created by xiezebin on 3/25/16.
 */
object DecisionTreeDr {

  val GLASS_FILE = "hdfs://cshadoop1/zxx140430/input/glass.data"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val data = sc.textFile(GLASS_FILE)
    val parsedData = data.map { line =>
      val parts = line.split(',')
      val len = parts.length
      val label = parts(len - 1).toDouble - 1           // last column is label in this case; range from [0, total num of class - 1]
      val features = parts.slice(1, parts.length - 1).map(_.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }

    // Split the data into training and test sets
    val splits = parsedData.randomSplit(Array(0.6, 0.4))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    val numClasses = 7                              // 7 classes in this case
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val decisionTreeModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = decisionTreeModel.predict(point.features)
      (point.label, prediction)
    }
    val accuracy = labelAndPreds.filter(r => r._1 == r._2).count().toDouble / testData.count()
    println("Accuracy = " + accuracy)
    println("Learned classification tree model:\n" + decisionTreeModel.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
  }
}
