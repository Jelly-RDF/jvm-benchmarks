package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, FlatData, FlatDataRdf4j}
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.convert.rdf4j.Rdf4jConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import org.apache.jena.riot
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.sparql.util.Context
import org.eclipse.rdf4j.rio
import org.eclipse.rdf4j.rio.WriterConfig

import java.io.{InputStream, OutputStream}
import scala.compiletime.uninitialized

trait FlatSerDes extends SerDes:
  protected var numStatements: Long = uninitialized
  protected var numStatementsRdf4j: Long = uninitialized
  protected var sourceData: FlatData = uninitialized
  protected var sourceDataRdf4j: FlatDataRdf4j = uninitialized

  protected final def loadData(path: String, streamType: String, statements: Int): Unit =
    val d = DataLoader.getSourceDataJellyFlat(path, streamType, if statements == 0 then None else Some(statements))
    numStatements = d.fold(identity, identity).size
    sourceData = d

  protected final def loadDataRdf4j(path: String, statements: Int): Unit =
    val d = DataLoader.getSourceDataJellyRdf4jFlat(path, if statements == 0 then None else Some(statements))
    numStatements = d.size
    sourceDataRdf4j = d

  protected final def serJena(format: riot.RDFFormat, ctx: Context, outputStream: OutputStream): Unit =
    val writer = StreamRDFWriter.getWriterStream(outputStream, format, ctx)
    writer.start()
    sourceData match
      case Left(triples) => FlatSerDesHelper.serJenaTriples(triples, writer)
      case Right(quads) => FlatSerDesHelper.serJenaQuads(quads, writer)
    writer.finish()

  protected final def serRdf4j(format: rio.RDFFormat, config: WriterConfig, outputStream: OutputStream): Unit =
    val writer = rio.Rio.createWriter(format, outputStream)
    writer.setWriterConfig(config)
    writer.startRDF()
    FlatSerDesHelper.serRdf4j(sourceDataRdf4j, writer)
    writer.endRDF()

//  protected final def desJelly(inputStream: InputStream, streamType: String): Unit =
//    val decoder = streamType match
//      case "triples" => JenaConverterFactory.triplesDecoder(None)
//      case "quads" => JenaConverterFactory.quadsDecoder(None)
//    Iterator.continually(RdfStreamFrame.parseDelimitedFrom(inputStream))
//      .takeWhile(_.isDefined)
//      .foreach(frame => frame.get.rows.foreach(decoder.ingestRowFlat))
//
//  protected final def desJellyRdf4j(inputStream: InputStream, streamType: String): Unit =
//    val decoder = streamType match
//      case "triples" => Rdf4jConverterFactory.triplesDecoder(None)
//      case "quads" => Rdf4jConverterFactory.quadsDecoder(None)
//    Iterator.continually(RdfStreamFrame.parseDelimitedFrom(inputStream))
//      .takeWhile(_.isDefined)
//      .foreach(frame => frame.get.rows.foreach(decoder.ingestRowFlat))
