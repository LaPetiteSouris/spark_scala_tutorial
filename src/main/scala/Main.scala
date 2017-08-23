package main.scala

import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Forest Cov")
    val sc = new SparkContext(conf)
    MainTrafficCluster.runCluster(sc)

  }


}
