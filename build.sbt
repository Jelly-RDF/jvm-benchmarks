ThisBuild / scalaVersion := "3.3.4"

// Uncomment to use SNAPSHOT releases of Jelly
resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

// val jellyV = "2.2.1"
val jellyV = "2.2.1+11-71065314-SNAPSHOT"
val jenaV = "5.2.0"
val rdf4jV = "5.0.2"

lazy val root = (project in file("."))
  .settings(
    name := "benchmarks",
    libraryDependencies ++= Seq(
      "eu.ostrzyciel.jelly" %% "jelly-grpc" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-stream" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-rdf4j" % jellyV,
      "org.json4s" %% "json4s-jackson" % "4.0.7",
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      "org.apache.pekko" %% "pekko-connectors-kafka" % "1.1.0",
      "org.apache.commons" % "commons-compress" % "1.27.1",
      "org.eclipse.rdf4j" % "rdf4j" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-ntriples" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-nquads" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-binary" % rdf4jV,
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
