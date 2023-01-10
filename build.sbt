ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.1"

val jellyV = "0.0.4"

lazy val root = (project in file("."))
  .settings(
    name := "benchmarks",
    libraryDependencies ++= Seq(
      "eu.ostrzyciel.jelly" %% "jelly-grpc" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-stream" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-rdf4j" % jellyV,
    ),
  )
