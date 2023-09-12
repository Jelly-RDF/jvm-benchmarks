package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.stream.{EncoderFlow, JellyOptionsFromTypesafe}
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
      val avgTime = (results.sum / results.size) / 1_000_000d
      println("Average time (ms): " + avgTime)
      println("Average " + size / avgTime + " kTriples/s")
      println("Times (ns): " + results)
      println()

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
