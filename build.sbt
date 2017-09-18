name := "scala_tuto"
version := "1.0"

scalaVersion := "2.11.4"


libraryDependencies ++= {
  val sparkVer = "2.1.0"
  val sparkXMLVersion = "0.4.1"
  val cloud9Version = "2.0.1"
  val blikiVer = "3.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-mllib" % sparkVer % "provided",
    "com.databricks" %% "spark-xml" % sparkXMLVersion,
    "info.bliki.wiki" % "bliki-core" % blikiVer

  )
}