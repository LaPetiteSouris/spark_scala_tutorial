package main.scala

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

object MainForestCov {

  def runCluster(sc: SparkContext) {

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val rawData = sc.textFile("hdfs:///user/ds/covtype.data")

    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
      val soil = values.slice(14, 54).indexOf(1.0).toDouble
      val featureVector = Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
      val label = values.last - 1
      LabeledPoint(label, featureVector)

    }

    val Array(trainData, cvData, test) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    test.cache()
    val numClasses = 7
    val categoricalFeaturesInfo = Map(10 -> 4, 11 -> 40)
    // For evaluation only
    evaluate(numClasses, categoricalFeaturesInfo, trainData, cvData)
    //val model = DecisionTree.trainClassifier(trainData.union(cvData), numClasses, categoricalFeaturesInfo,
    //  "gini", 20, 100)
    //val metrics = measureMetrics(model, test)
    //val accuracy = metrics.accuracy
    //println("Accuracy of the hyperparameters is %s".format(accuracy))


  }

  def evaluate(numClasses: Int, categoricalFeaturesInfo: Map[Int, Int], trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]) {
    // Evaluation phase
    val evaluations =
      for (impurity <- Array("gini", "entropy");
           maxDepth <- Array(1, 20);
           maxBins <- Array(40, 100))
        yield {
          val model = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
            impurity, maxDepth, maxBins)
          val metrics = measureMetrics(model, cvData)
          val accuracy = metrics.accuracy
          (impurity, maxDepth, maxBins, accuracy)
        }
    evaluations.sortBy(_._4).foreach(println)
  }

  def measureMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionAndLabel = data.map { example =>
      val prediction = model.predict(example.features)
      (prediction, example.label)
    }
    new MulticlassMetrics(predictionAndLabel)

  }

}
