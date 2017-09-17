package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Forest Cov")
    conf.set("spark.driver.maxResultSize", "3g")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    WikiAnalytics.runCluster(sc)

  }


}
