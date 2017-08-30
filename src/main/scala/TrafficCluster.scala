package main.scala

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd._

object MainTrafficCluster {

  def distance(a: Vector, b: Vector): Double = {
    Vectors.sqdist(a, b)

  }


  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def runCluster(sc: SparkContext) {

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val rawData = sc.textFile("hdfs:///user/ds/traffic.data")
    println("Starting traffic")
    val labelandData = rawData.map { line =>
      val lineSplit = line.split(",")

      // Temporarily remove string categorical features
      // Immutable is better than mutable filtering
      val lineFiltered = lineSplit.indices.collect { case i if !(i == 1 || i == 2 || i == 3 || i == (lineSplit.length - 1)) => lineSplit(i) }
      val label = lineSplit.last
      val values = lineFiltered.map(_.toDouble).toArray
      // Feature vector
      val featureVector = Vectors.dense(values)
      (label, featureVector)
    }

    val data = labelandData.values.cache()

    // Creating a Scaler model that standardizes with both mean and SD
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(data)
    // Scale features using the scaler model

    val scaledFeaturesData = scaler.transform(data)
    val dataNormalized = scaledFeaturesData.cache()


    (60 to 120 by 10).par.map(k => (k, clusteringScore(dataNormalized, k))).foreach(println)
  }


  def clusteringScore(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }


}
