package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.core.JellyOptions
import org.apache.jena.riot.RDFFormat

object Constants:
  // Formats that do not support the Jena streaming API
  val jenaFormats: Map[String, (RDFFormat, Option[RDFFormat])] = Map(
    // Use a non-pretty variant of RDF/XML
    "jena-rdf-xml" -> (RDFFormat.RDFXML_PLAIN, None),
    "jena-json-ld" -> (RDFFormat.JSONLD_PLAIN, Some(RDFFormat.JSONLD_PLAIN)),
    "jena-turtle-pretty" -> (RDFFormat.TURTLE_PRETTY, Some(RDFFormat.TRIG_PRETTY)),
  )

  // Formats that support the Jena streaming API for writing
  val jenaStreamFormats: Map[String, (RDFFormat, Option[RDFFormat])] = Map(
    "jena-turtle-blocks" -> (RDFFormat.TURTLE_BLOCKS, Some(RDFFormat.TRIG_BLOCKS)),
    "jena-nt" -> (RDFFormat.NTRIPLES, Some(RDFFormat.NQUADS)),
    "jena-protobuf" -> (RDFFormat.RDF_PROTO, Some(RDFFormat.RDF_PROTO)),
    "jena-thrift" -> (RDFFormat.RDF_THRIFT, Some(RDFFormat.RDF_THRIFT)),
  )

  val jellyOptions = Map(
    "jelly-big" -> JellyOptions.bigGeneralized,
    "jelly-big-noprefix" -> JellyOptions.bigGeneralized.withMaxPrefixTableSize(0),
    "jelly-small" -> JellyOptions.smallGeneralized,
    "jelly-small-noprefix" -> JellyOptions.smallGeneralized.withMaxPrefixTableSize(0),
  )
