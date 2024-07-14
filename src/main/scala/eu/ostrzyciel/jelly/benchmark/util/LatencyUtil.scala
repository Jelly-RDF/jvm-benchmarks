package eu.ostrzyciel.jelly.benchmark.util

import scala.collection.mutable
import scala.concurrent.duration.*

object LatencyUtil:
  private val testRepeats = 1 to ConfigManager.benchmarkNetworkRepeats

  def run(call: (Option[FiniteDuration], Int) => Seq[(Long, Long)], results: mutable.Map[String, Seq[(Long, Long)]]):
  Unit =
    for
      (elements, interval) <- ConfigManager.benchmarkLatencyCases
      run <- testRepeats
    do
      val key = s"$interval $run"
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println("Running: " + key)
      val result = call(interval, elements)
      results.update(key, result)
      val avgLat = (result.map((a, b) => b - a).sum / 1000.0) / result.size
      println(s"Average latency: $avgLat microseconds")
