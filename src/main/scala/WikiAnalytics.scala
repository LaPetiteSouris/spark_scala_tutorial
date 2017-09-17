package main.scala

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import com.databricks.spark.xml.XmlInputFormat

object WikiAnalytics {

  def runCluster(sc: SparkContext) {
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val records = sc.newAPIHadoopFile(
      "hdfs:///user/dt/enwiki.xml",
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    val rawXmls = records.map(p => p._2.toString)
    rawXmls.collect().foreach(println)
  }



}
