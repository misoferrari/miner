ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.10"

lazy val root = (project in file("."))
  .settings(
    name := "Miner"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp-gpu" % "2.4.3"
