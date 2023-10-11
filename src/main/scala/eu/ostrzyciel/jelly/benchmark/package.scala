package eu.ostrzyciel.jelly

import scala.collection.mutable
import scala.concurrent.duration.*

package object benchmark:

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



