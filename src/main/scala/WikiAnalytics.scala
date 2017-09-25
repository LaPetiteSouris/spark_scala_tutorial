package main.scala

import java.io.{ByteArrayInputStream, FileOutputStream, PrintStream}
import java.util.Properties

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import com.databricks.spark.xml.XmlInputFormat
import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXException
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer


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

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
  : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def isOnlyLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }

  def termFrequency(term: Seq[String]): Map[String, Int] = {
    term.groupBy(i => i).mapValues(_.size)
  }

  def documentFrequencies(docTermFreqs: RDD[Map[String, Int]]): Map[String, Int] = {
    val zero = Map[String, Int]()

    def merge(dfs: Map[String, Int], tfs: Map[String, Int]): Map[String, Int] = {
      tfs.keySet.map(k => {
        var count = 1
        if (dfs.getOrElse(k, 0) != 0) {
          count = dfs.getOrElse(k, 0) + 1
        }
        Map(k -> count)
      }).reduceLeft(_ ++ _)
      dfs ++ tfs
    }

    def mergeFinalMaps(map1: Map[String, Int], map2: Map[String, Int]): Map[String, Int] = {
      map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
    }

    docTermFreqs.aggregate(zero)(merge, mergeFinalMaps)

  }


  def runCluster(sc: SparkContext) {
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
    val stopWords = sc.broadcast(
      scala.io.Source.fromFile("stopwords.txt").getLines().toSet).value

    val records = sc.newAPIHadoopFile(
      "hdfs:///user/dt/enwiki.xml",
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    val rawContents = records.map(p => p._2.toString)
    val plainText = rawContents.flatMap(parsePages)

    val lemmatized: RDD[Seq[String]] = plainText.mapPartitions(it => {
      val pipeline = createNLPPipeline()
      it.map { case (title, contents) =>
        plainTextToLemmas(contents, stopWords, pipeline)
      }
    })
    val docTermFreq: RDD[Map[String, Int]] = lemmatized.map(term => termFrequency(term))
    docTermFreq.cache()
    docTermFreq.take(30).foreach(println)
  }


}
