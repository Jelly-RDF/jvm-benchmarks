package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.core.JellyOptions
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamOptions, RdfStreamType}
import org.apache.jena.riot.RDFFormat

object Experiments:
  val REPEATS = 1

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

  def getFormat(exp: String, streamType: String): RDFFormat =
    val tuple = jenaFormats(exp)
    if streamType == "triples" then tuple(0) else tuple(1)

  def getJellyOpts(exp: String, streamType: String): RdfStreamOptions =
    jellyOptions(exp).withStreamType(
      streamType match
        case "triples" => RdfStreamType.TRIPLES
        case "graphs" => RdfStreamType.GRAPHS
        case "quads" => RdfStreamType.QUADS
    )
