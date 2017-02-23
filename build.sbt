//import AssemblyKeys._ // put this at the top of the file

lazy val commonSettings = Seq(
  organization := "com.kiran",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")) //.settings(commonSettings: _*).
  .settings(
    commonSettings,
    name := "km_scala"
    //libraryDependencies += derby
  )
  .enablePlugins(AssemblyPlugin)
/*
lazy val utils = (project in file("utils")).
  settings(commonSettings: _*).
  settings(
    assemblyJarName in assembly := "utils.jar"
    // more settings here ...
  )
*/
//name := "km_scala"
//version := "1.0"

//scalaVersion := "2.12.1"
//scalaVersion := "2.11.8" //Using this to avoid incompatibility

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {x => x.data.getName.matches("sbt.*") || x.data.getName.matches(".*macros.*")}
}

//assemblySettings

assemblyJarName in assembly := "spark-assembly.jar"

// https://mvnrepository.com/artifact/org.scala-sbt/sbt
//libraryDependencies += "org.scala-sbt" % "sbt" % "1.0.0-M4"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter_2.11
//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive_2.11
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.0.0"


// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
//libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.7"

//-------NOT NEEDED ----Changing to use older version 1.3.1 for org.apache.spark.sql.SQLContext
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"



// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
//libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"

//**********************Hive****************
// https://mvnrepository.com/artifact/org.apache.hive/hive-common
//libraryDependencies += "org.apache.hive" % "hive-common" % "2.0.0"


// https://mvnrepository.com/artifact/org.apache.hive/hive-exec
//libraryDependencies += "org.apache.hive" % "hive-exec" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.hive/hive-metastore
//libraryDependencies += "org.apache.hive" % "hive-metastore" % "2.0.0"


//**********************Hive END*************


assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "objenesis", xs @ _*) => MergeStrategy.last
  case PathList("com", "datanucleus", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => old(x)
}
}