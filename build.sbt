ThisBuild / scalaVersion := "3.2.1"

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

val jellyV = "0.0.5+1-57ffd9b7-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "benchmarks",
    libraryDependencies ++= Seq(
      "eu.ostrzyciel.jelly" %% "jelly-grpc" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-stream" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-rdf4j" % jellyV,
      "org.json4s" %% "json4s-jackson" % "4.0.6",
    ),
  )
