package eu.ostrzyciel.jelly.benchmark.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*

object ConfigManager:
  val config: Config = ConfigFactory.load("benchmark.conf").withFallback(ConfigFactory.load())
  
  val benchmarkOutputDir: String = config.getString("jelly.benchmark.output-dir")

  // Number of repeats for experiments that are CPU-bound
  val benchmarkRepeats: Int = config.getInt("jelly.benchmark.repeats")
  // Number of repeats for experiments that are network-bound
  val benchmarkNetworkRepeats: Int = config.getInt("jelly.benchmark.network-repeats")
  
  case class LatencyCase(messages: Int, interval: Option[FiniteDuration], burst: Option[Int])

  val benchmarkLatencyCases: Seq[LatencyCase] = config.getConfigList("jelly.benchmark.latency-cases")
    .asScala
    .map { c =>
      val d = c.getDuration("interval").toScala
      LatencyCase(
        messages = c.getInt("messages"),
        interval = if d.length == 0 then None else Some(d),
        burst = if c.hasPath("burst") then Some(c.getInt("burst")) else None
      )
    }
    .toSeq
  
  object enabledFormats:
    val jena: Seq[String] = config.getString("jelly.enabled-formats.jena")
      .split(',')
      .map(_.trim.prependedAll("jena-"))
    val jenaStreaming: Seq[String] = config.getString("jelly.enabled-formats.jena-streaming")
      .split(',')
      .map(_.trim.prependedAll("jena-"))
    val jelly: Seq[String] = config.getString("jelly.enabled-formats.jelly")
      .split(',')
      .map(_.trim.prependedAll("jelly-"))
