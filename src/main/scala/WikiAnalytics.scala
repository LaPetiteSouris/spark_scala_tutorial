package main.scala

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import com.databricks.spark.xml.XmlInputFormat

object WikiAnalytics {

  def parseContentofPage(str: String): String = {
    val xmlContent = scala.xml.XML.loadString(str)
    val revision = xmlContent \\ "text"
    revision.text
  }

  def runCluster(sc: SparkContext) {
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val records = sc.newAPIHadoopFile(
      "hdfs:///user/dt/enwiki.xml",
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    val rawContents = records.map(p => parseContentofPage(p._2.toString))

    rawContents.take(20).foreach(println)
  }



}
