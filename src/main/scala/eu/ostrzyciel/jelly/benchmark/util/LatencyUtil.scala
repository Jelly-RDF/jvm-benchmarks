package eu.ostrzyciel.jelly.benchmark.util

import scala.collection.mutable
import scala.concurrent.duration.*

object LatencyUtil:
  private val intervals = Seq(
    // Run the constrained variant only on 1000 messages to avoid waiting for a few lifetimes
    (Some(10.millis), 1_000),
    (Some(1.milli), 1_000),
    (Some(100.micros), 1_000),
    (None, 10_000),
  )
  private val testRuns = 1 to ConfigManager.benchmarkNetworkRepeats

  def run(call: (Option[FiniteDuration], Int) => Seq[(Long, Long)], results: mutable.Map[String, Seq[(Long, Long)]]):
  Unit =
    for
      (interval, elements) <- intervals
      run <- testRuns
    do
      val key = s"$interval $run"
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println("Running: " + key)
      val result = call(interval, elements)
      results.update(key, result)
      val avgLat = (result.map((a, b) => b - a).sum / 1000.0) / result.size
      println(s"Average latency: $avgLat microseconds")
