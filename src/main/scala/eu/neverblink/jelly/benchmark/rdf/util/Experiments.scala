package eu.neverblink.jelly.benchmark.rdf.util

import eu.neverblink.jelly.benchmark.util.ConfigManager
import eu.neverblink.jelly.core.proto.v1.*
import org.apache.jena.riot.RDFFormat
import org.eclipse.rdf4j.rio

import scala.util.Random

object Experiments:
  // Set seed to current time
  Random.setSeed(System.currentTimeMillis())
  
  def getFormatKeysToTest(jena: Boolean, jenaStreaming: Boolean, jelly: Boolean, rdf4j: Boolean, streamType: String): 
  Seq[String] =
    val keys = Seq.newBuilder[String]
    if jena then keys ++= Constants.jenaFormats
      .filter(_._2._2.isDefined || streamType == "triples").keys
      .filter(ConfigManager.enabledFormats.jena.contains)
    if jenaStreaming then keys ++= Constants.jenaStreamFormats
      .filter(_._2._2.isDefined || streamType == "triples").keys
      .filter(ConfigManager.enabledFormats.jenaStreaming.contains)
    if jelly then keys ++= Constants.jellyOptions.keys
      .filter(ConfigManager.enabledFormats.jelly.contains)
    if rdf4j then keys ++= Constants.rdf4jFormats.keys
      .filter(ConfigManager.enabledFormats.rdf4j.contains)
    Random.shuffle(keys.result())
  
  def getJenaFormat(exp: String, streamType: String): Option[RDFFormat] =
    val tuple = Constants.jenaFormats.getOrElse(exp, Constants.jenaStreamFormats(exp))
    if streamType == "triples" then Some(tuple(0)) else tuple(1)
    
  def getRdf4jFormat(exp: String, streamType: String): Option[rio.RDFFormat] =
    val tuple = Constants.rdf4jFormats(exp)
    if streamType == "triples" then Some(tuple._1) else tuple._2

  def getJellyOpts(exp: String, streamType: String, grouped: Boolean): RdfStreamOptions =
    val (physType, logicType) = streamType match
      case "triples" => (
        PhysicalStreamType.TRIPLES, 
        if grouped then LogicalStreamType.GRAPHS else LogicalStreamType.FLAT_TRIPLES
      )
      case "graphs" => (
        PhysicalStreamType.GRAPHS,
        if grouped then LogicalStreamType.DATASETS else LogicalStreamType.FLAT_QUADS
      )
      case "quads" => (
        PhysicalStreamType.QUADS,
        if grouped then LogicalStreamType.DATASETS else LogicalStreamType.FLAT_QUADS
      )
    Constants.jellyOptions(exp).clone
      .setPhysicalType(physType)
      .setLogicalType(logicType)
