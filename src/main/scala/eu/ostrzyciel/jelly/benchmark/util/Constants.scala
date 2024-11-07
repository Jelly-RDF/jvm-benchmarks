package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.core.JellyOptions
import org.apache.jena.riot
import org.eclipse.rdf4j.rio

object Constants:
  // Formats that do not support the Jena streaming API
  val jenaFormats: Map[String, (riot.RDFFormat, Option[riot.RDFFormat])] = Map(
    // Use a non-pretty variant of RDF/XML
    "jena-rdf-xml" -> (riot.RDFFormat.RDFXML_PLAIN, None),
    "jena-json-ld" -> (riot.RDFFormat.JSONLD_PLAIN, Some(riot.RDFFormat.JSONLD_PLAIN)),
    "jena-turtle-pretty" -> (riot.RDFFormat.TURTLE_PRETTY, Some(riot.RDFFormat.TRIG_PRETTY)),
  )

  // Formats that support the Jena streaming API for writing
  val jenaStreamFormats: Map[String, (riot.RDFFormat, Option[riot.RDFFormat])] = Map(
    "jena-turtle-blocks" -> (riot.RDFFormat.TURTLE_BLOCKS, Some(riot.RDFFormat.TRIG_BLOCKS)),
    "jena-nt" -> (riot.RDFFormat.NTRIPLES, Some(riot.RDFFormat.NQUADS)),
    "jena-protobuf" -> (riot.RDFFormat.RDF_PROTO, Some(riot.RDFFormat.RDF_PROTO)),
    "jena-thrift" -> (riot.RDFFormat.RDF_THRIFT, Some(riot.RDFFormat.RDF_THRIFT)),
  )

  val jellyOptions = Map(
    "jelly-big" -> JellyOptions.bigGeneralized,
    "jelly-big-noprefix" -> JellyOptions.bigGeneralized.withMaxPrefixTableSize(0),
    "jelly-small" -> JellyOptions.smallGeneralized,
    "jelly-small-noprefix" -> JellyOptions.smallGeneralized.withMaxPrefixTableSize(0),
  )

  val rdf4jFormats: Map[String, (rio.RDFFormat, Option[rio.RDFFormat])] = Map(
    "rdf4j-nt" -> (rio.RDFFormat.NTRIPLES, Some(rio.RDFFormat.NQUADS)),
    "rdf4j-binary" -> (rio.RDFFormat.BINARY, Some(rio.RDFFormat.BINARY)),
  )
