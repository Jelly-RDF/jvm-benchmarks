package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.core.JellyOptions
import eu.ostrzyciel.jelly.core.proto.v1.*
import org.apache.jena.riot.RDFFormat

object Experiments:
  // Number of repeats for experiments that are CPU-bound
  val REPEATS = 15
  // Number of repeats for experiments that are network-bound
  val NETWORK_REPEATS = 8

  val jenaFormats = Map(
    // Use a non-pretty variant of RDF/XML
    // RDF/XML is abysmally slow and it doesn't even support quads...
    // "jena-rdf-xml" -> (RDFFormat.RDFXML_PLAIN, RDFFormat.RDFXML_PLAIN),
    "jena-turtle-pretty" -> (RDFFormat.TURTLE_PRETTY, RDFFormat.TRIG_PRETTY),
    "jena-turtle-blocks" -> (RDFFormat.TURTLE_BLOCKS, RDFFormat.TRIG_BLOCKS),
    "jena-nt" -> (RDFFormat.NTRIPLES, RDFFormat.NQUADS),
    "jena-protobuf" -> (RDFFormat.RDF_PROTO, RDFFormat.RDF_PROTO),
    "jena-thrift" -> (RDFFormat.RDF_THRIFT, RDFFormat.RDF_THRIFT),
    // The TriX implementation in Jena is kinda buggy, it breaks on some encoding issues
    // "jena-trix" -> (RDFFormat.TRIX, RDFFormat.TRIX),
  )

  val jellyOptions = Map(
    "jelly-big" -> JellyOptions.bigGeneralized,
    "jelly-small" -> JellyOptions.smallGeneralized,
    "jelly-small-noprefix" -> JellyOptions.smallGeneralized.withMaxPrefixTableSize(0),
    "jelly-small-norepeat" -> JellyOptions.smallGeneralized.withUseRepeat(false),
    "jelly-small-noprefix-norepeat" -> JellyOptions.smallGeneralized
      .withMaxPrefixTableSize(0)
      .withUseRepeat(false),
  )

  val jellyOptionsSmall = jellyOptions.filterNot(_._1.contains("norepeat"))

  def getFormat(exp: String, streamType: String): RDFFormat =
    val tuple = jenaFormats(exp)
    if streamType == "triples" then tuple(0) else tuple(1)

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
    jellyOptions(exp).copy(physicalType = physType, logicalType = logicType)
