package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, FlatData}
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.system.StreamRDFWriter

import java.io.{InputStream, OutputStream}

trait FlatSerDes extends SerDes:
  protected var numStatements: Long = _
  protected var sourceData: FlatData = _

  protected final def loadData(path: String, streamType: String): Unit =
    val d = DataLoader.getSourceDataJellyFlat(path, streamType)
    numStatements = d.fold(identity, identity).size
    sourceData = d

  protected final def serJelly(
    opt: RdfStreamOptions, closure: RdfStreamFrame => Unit, frameSize: Int
  ): Unit =
    val encoder = JenaConverterFactory.encoder(opt)
    val rows = sourceData match
      case Left(triples) => triples.flatMap(encoder.addTripleStatement)
      case Right(quads) => quads.flatMap(encoder.addQuadStatement)
    rows
      .grouped(frameSize)
      .map(RdfStreamFrame(_))
      .foreach(closure)

  protected final def serJena(format: RDFFormat, outputStream: OutputStream): Unit =
    val writer = StreamRDFWriter.getWriterStream(outputStream, format.getLang)
    writer.start()
    sourceData match
      case Left(triples) => triples.foreach(writer.triple)
      case Right(quads) => quads.foreach(writer.quad)
    writer.finish()

  protected final def desJelly(inputStream: InputStream, streamType: String): Unit =
    val decoder = streamType match
      case "triples" => JenaConverterFactory.triplesDecoder(None)
      case "quads" => JenaConverterFactory.quadsDecoder(None)
    Iterator.continually(RdfStreamFrame.parseDelimitedFrom(inputStream))
      .takeWhile(_.isDefined)
      .map(frame => frame.get.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})
