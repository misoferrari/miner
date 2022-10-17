ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.10"

lazy val root = (project in file("."))
  .settings(
    name := "Miner"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"
libraryDependencies += "org.vegas-viz" %% "vegas-spark" % "0.3.11"