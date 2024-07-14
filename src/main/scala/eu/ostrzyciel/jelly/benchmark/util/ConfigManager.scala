package eu.ostrzyciel.jelly.benchmark.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*

object ConfigManager:
  val config: Config = ConfigFactory.load("benchmark.conf").withFallback(ConfigFactory.load())
  
  val debugOutputDir: String = config.getString("jelly.debug.output-dir")

  // Number of repeats for experiments that are CPU-bound
  val benchmarkRepeats: Int = config.getInt("jelly.benchmark.repeats")
  // Number of repeats for experiments that are network-bound
  val benchmarkNetworkRepeats: Int = config.getInt("jelly.benchmark.network-repeats")

  val benchmarkLatencyCases: Seq[(Int, Option[FiniteDuration])] = config.getConfigList("jelly.benchmark.latency-cases")
    .asScala
    .map { c =>
      val d = c.getDuration("interval").toScala
      (
        c.getInt("messages"),
        if d.length == 0 then None else Some(d)
      )
    }
    .toSeq
  
  object enabledFormats:
    val jena: Seq[String] = config.getStringList("jelly.enabled-formats.jena").asScala.toSeq
      .map(_.prependedAll("jena-"))
    val jenaStreaming: Seq[String] = config.getStringList("jelly.enabled-formats.jena-streaming").asScala.toSeq
      .map(_.prependedAll("jena-"))
    val jelly: Seq[String] = config.getStringList("jelly.enabled-formats.jelly").asScala.toSeq
      .map(_.prependedAll("jelly-"))
