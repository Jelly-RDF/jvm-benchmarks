package eu.ostrzyciel.jelly

import eu.ostrzyciel.jelly.core.JellyOptions
import org.apache.jena.riot.RDFFormat
import org.apache.jena.sys.JenaSystem

import scala.collection.mutable
import scala.concurrent.duration.*

package object benchmark:
  val REPEATS = 15

  val jenaFormats = Map(
    // Use a non-pretty variant of RDF/XML
    // RDF/XML is abysmally slow and it doesn't even support quads...
    // "jena-rdf-xml" -> (RDFFormat.RDFXML_PLAIN, RDFFormat.RDFXML_PLAIN),
    "jena-turtle-pretty" -> (RDFFormat.TURTLE_PRETTY, RDFFormat.TRIG_PRETTY),
    "jena-turtle-blocks" -> (RDFFormat.TURTLE_BLOCKS, RDFFormat.TRIG_BLOCKS),
    "jena-nt" -> (RDFFormat.NTRIPLES, RDFFormat.NQUADS),
    "jena-protobuf" -> (RDFFormat.RDF_PROTO, RDFFormat.RDF_PROTO),
    "jena-thrift" -> (RDFFormat.RDF_THRIFT, RDFFormat.RDF_THRIFT),
    // The TriX implementation in Jena is kinda buggy, it breaks on some encoding issues
    // "jena-trix" -> (RDFFormat.TRIX, RDFFormat.TRIX),
  )

  val jellyOptions = Map(
    "jelly-big" -> JellyOptions.bigGeneralized,
    "jelly-small" -> JellyOptions.smallGeneralized,
    "jelly-small-noprefix" -> JellyOptions.smallGeneralized.withMaxPrefixTableSize(0),
    "jelly-small-norepeat" -> JellyOptions.smallGeneralized.withUseRepeat(false),
    "jelly-small-noprefix-norepeat" -> JellyOptions.smallGeneralized
      .withMaxPrefixTableSize(0)
      .withUseRepeat(false),
  )

  object LatencyUtil:
    // TODO: figure out what to do with this
    val intervals = Seq(
      10.millis,
      1.milli,
      100.micros,
    )
    val messageCount = 1000
    // required triples: 1000*200 = 200kT
    val mSizes = Seq(5, 50, 200)
    val testRuns = 1 to 3

    def run(call: (interval: FiniteDuration, mSize: Int) => Seq[(Long, Long)],
            results: mutable.Map[String, Any]):
    Unit =
      for
        interval <- intervals
        mSize <- mSizes
        run <- testRuns
      do
        val key = s"$interval $mSize $run"
        println("Sleeping 3 seconds...")
        Thread.sleep(3000)
        println("Running: " + key)
        val result = call.apply(interval, mSize)
        results.update(key, result)
        val avgLat = (result.map((a, b) => b - a).sum / 1000.0) / result.size
        println(s"Average latency: $avgLat microseconds")



