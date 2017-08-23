package main.scala

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext


object MainTrafficCluster {

  def runCluster(sc: SparkContext) {

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val rawData = sc.textFile("hdfs:///user/ds/traffic.data")

    val data = rawData.map { line =>
      val lineSplit = line.split(",")
      val lineFiltered = lineSplit.indices.collect { case i if i != 1 || i != 3 || i == line.length - 1 => lineSplit(i) }
      val label = lineSplit.last
      val values = lineFiltered.map(_.toDouble).toArray
      val featureVector = Vectors.dense(values)
      (label, featureVector)
    }
    print(data)


  }


}
