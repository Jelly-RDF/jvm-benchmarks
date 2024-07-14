package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.benchmark.util.ConfigManager.LatencyCase
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.collection.mutable
import scala.concurrent.duration.*

object LatencyUtil:
  private val testRepeats = 1 to ConfigManager.benchmarkNetworkRepeats

  def run(call: LatencyCase => Seq[(Long, Long)], results: mutable.Map[String, Seq[(Long, Long)]]):
  Unit =
    for
      latencyCase <- ConfigManager.benchmarkLatencyCases
      run <- testRepeats
    do
      val key = if latencyCase.interval.isDefined then
        s"${latencyCase.interval.get.toMicros / 1000f} ms, burst ${latencyCase.burst.getOrElse(1)}, $run"
      else s"unconstrained, burst ${latencyCase.burst.getOrElse(1)}, $run"
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println()
      println("Running: " + key)
      val result = call(latencyCase)
      results.update(key, result)
      val meanLat = (result.map((a, b) => b - a).sum / 1000.0) / result.size
      val meanTZeroDelta = (result.last._1 - result.head._1) / (1000.0 * (result.size - 1))
      val meanTOneDelta = (result.last._2 - result.head._2) / (1000.0 * (result.size - 1))
      println(s"Mean latency: $meanLat microseconds")
      println(s"Mean t0 delta: $meanTZeroDelta microseconds")
      println(s"Mean t1 delta: $meanTOneDelta microseconds")

  class LatencyStreamService(
    data: GroupedData, latencyCase: LatencyCase, timestamps: mutable.ArrayBuffer[Long]
  ) extends RdfStreamServiceFromData(data):
    override protected def source[T](s: Seq[T]): Source[T, NotUsed] =
      latencyCase.interval match
        case Some(i) => Source(s)
          .take(latencyCase.messages)
          .grouped(latencyCase.burst.getOrElse(1))
          .zip(Source.tick(0.seconds, i, 0))
          .mapConcat((el, _) => {
            val time = System.nanoTime()
            for _ <- 1 to el.size do
              timestamps += time
            el
          })
        case None => Source(s)
          .take(latencyCase.messages)
          .map(el => {
            timestamps += System.nanoTime()
            el
          })
