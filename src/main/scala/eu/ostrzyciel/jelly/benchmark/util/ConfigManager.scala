package eu.ostrzyciel.jelly.benchmark.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.*

object ConfigManager:
  val config: Config = ConfigFactory.load("benchmark.conf").withFallback(ConfigFactory.load())
  
  val debugOutputDir: String = config.getString("jelly.debug.output-dir")

  // Number of repeats for experiments that are CPU-bound
  val benchmarkRepeats: Int = config.getInt("jelly.benchmark.repeats")
  // Number of repeats for experiments that are network-bound
  val benchmarkNetworkRepeats: Int = config.getInt("jelly.benchmark.network-repeats")
  
  object enabledFormats:
    val jena: Seq[String] = config.getStringList("jelly.enabled-formats.jena").asScala.toSeq
      .map(_.prependedAll("jena-"))
    val jenaStreaming: Seq[String] = config.getStringList("jelly.enabled-formats.jena-streaming").asScala.toSeq
      .map(_.prependedAll("jena-"))
    val jelly: Seq[String] = config.getStringList("jelly.enabled-formats.jelly").asScala.toSeq
      .map(_.prependedAll("jelly-"))
