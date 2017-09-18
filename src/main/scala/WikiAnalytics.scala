package main.scala

import java.io.ByteArrayInputStream

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import com.databricks.spark.xml.XmlInputFormat
import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXException


case class Page(title: String, text: String, isCategory: Boolean, isFile: Boolean, isTemplate: Boolean)

case class WrappedPage(var page: WikiArticle = new WikiArticle) {}

class SetterArticleFilter(val wrappedPage: WrappedPage) extends IArticleFilter {
  @throws(classOf[SAXException])
  def process(page: WikiArticle, siteinfo: Siteinfo) {
    wrappedPage.page = page
  }
}

object WikiAnalytics {

  def parsePages(text: String): Option[(String, String)] = {

    val wrappedPage = new WrappedPage
    //The parser occasionally exceptions out, we ignore these
    try {
      val parser = new WikiXMLParser(new ByteArrayInputStream(text.getBytes), new SetterArticleFilter(wrappedPage))
      parser.parse()
    } catch {
      case e: Exception =>
    }
    val page = wrappedPage.page
    if (page.getText != null && page.getTitle != null
      && page.getId != null && page.getRevisionId != null
      && page.getTimeStamp != null) {
      Some((page.getTitle, page.getText))
    } else {
      None
    }

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

    val rawContents = records.map(p => p._2.toString)
    val plainText = rawContents.flatMap(parsePages)

    plainText.take(30).foreach(println)
  }


}
