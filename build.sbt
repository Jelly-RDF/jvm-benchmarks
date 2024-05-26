ThisBuild / scalaVersion := "3.3.0"

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

val jellyV = "0.7.0"

lazy val root = (project in file("."))
  .settings(
    name := "benchmarks",
    libraryDependencies ++= Seq(
      "eu.ostrzyciel.jelly" %% "jelly-grpc" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-stream" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      // "eu.ostrzyciel.jelly" %% "jelly-rdf4j" % jellyV,
      "org.json4s" %% "json4s-jackson" % "4.0.7",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      // https://jena.apache.org/documentation/notes/jena-repack.html
      case PathList("META-INF", "services", xs@_*) => MergeStrategy.concat
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
  )
