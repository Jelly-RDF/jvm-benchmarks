package eu.neverblink.jelly.benchmark.util

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.nio.file.{Files, Paths}

object Util:
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

  def printSpeed(size: Long, times: Map[String, Iterable[Long]]): Unit =
    println("Statements: " + size)
    for exp -> results <- times do
      println("Experiment: " + exp)
      val avgTime = (results.sum / results.size.toDouble) / 1_000_000d
      println("Average time (ms): " + avgTime)
      println("Average " + size / avgTime + " kTriples/s")
      println("Times (ns): " + results)
      println()

  def saveRunInfo[T](name: String, result: T): Unit =
    given DefaultFormats.type = org.json4s.DefaultFormats
    val results = Map(
      "name" -> name,
      "result" -> result,
    )

    val filename = ConfigManager.benchmarkOutputDir +
      name + "_" + System.currentTimeMillis() / 1000 + ".json"
    val path = Paths.get(filename)
    path.getParent.toFile.mkdirs()
    Files.writeString(path, Serialization.write(results))
