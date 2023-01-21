package eu.ostrzyciel.jelly

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamOptions
import org.apache.jena.riot.Lang
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import eu.ostrzyciel.jelly.stream.{EncoderFlow, JellyOptionsFromTypesafe}

import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.concurrent.duration.*

package object benchmark:
  val REPEATS = 15

  val jenaLangs = Map(
    "rdf-xml" -> Lang.RDFXML,
    "turtle" -> Lang.TURTLE,
    "n3" -> Lang.NTRIPLES,
    "jena-proto" -> Lang.RDFPROTO,
  )

  object LatencyUtil:
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


  def time[R](expr: => R): (R, Long) =
    val t0 = System.nanoTime()
    val result = expr
    val t1 = System.nanoTime()
    (result, t1 - t0)

  def time(expr: => Unit): Long =
    val t0 = System.nanoTime()
    expr
    val t1 = System.nanoTime()
    t1 - t0

  def printSpeed(size: Long, times: Iterable[Long]): Unit =
    println("Triples: " + size)
    val avgTime = (times.sum / times.size) / 1_000_000d
    println("Average time (ms): " + avgTime)
    println("Average " + size / avgTime + " kTriples/s")
    println("Times (ns): " + times)

  def saveRunInfo[T](name: String, config: Config, result: T): Unit =
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val results = Map(
      "name" -> name,
      "stream_opts" -> JellyOptionsFromTypesafe.fromTypesafeConfig(config),
      "encoder_opts" -> EncoderFlow.Options(config),
      "result" -> result,
    )

    val filename = config.getString("jelly.debug.output-dir") +
      name + "_" + System.currentTimeMillis() / 1000 + ".json"
    Files.writeString(
      Paths.get(filename),
      Serialization.write(results)
    )
