package eu.neverblink.jelly.benchmark.traits

import eu.neverblink.jelly.benchmark.util.{DataLoader, GroupedData, GroupedDataRdf4j}
import eu.neverblink.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.{ConverterFactory, ProtoEncoder}
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions, RdfStreamRow}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio

import java.io.OutputStream
import scala.collection.mutable.ListBuffer
import scala.compiletime.uninitialized

trait GroupedSerDes extends SerDes:
  import eu.ostrzyciel.jelly.convert.jena.{*, given}
  import eu.ostrzyciel.jelly.convert.rdf4j.Rdf4jConverterFactory

  protected var numElements: Long = uninitialized
  protected var numStatements: Long = uninitialized
  protected var numElementsRdf4j: Long = uninitialized
  protected var numStatementsRdf4j: Long = uninitialized
  protected var sourceData: GroupedData = uninitialized
  protected var sourceDataRdf4j: GroupedDataRdf4j = uninitialized

  protected final def loadData(path: String, streamType: String, elementSize: Int, elements: Option[Int]): Unit =
    val d = DataLoader.getSourceData(path, streamType, elementSize, elements)
    numStatements = d._1
    numElements = d._2
    sourceData = d._3

  protected final def loadDataRdf4j(path: String, elementSize: Int, elements: Option[Int]): Unit =
    val d = DataLoader.getSourceDataRdf4j(path, elementSize, elements)
    numStatementsRdf4j = d._1
    numElementsRdf4j = d._2
    sourceDataRdf4j = d._3

  private final def triplesToFrame[T](
    buf: ListBuffer[RdfStreamRow], enc: ProtoEncoder[?, T, ?, ?], triples: Iterable[T]
  ): RdfStreamFrame =
    // TRIPLES
    triples.foreach(enc.addTripleStatement)
    val frame = RdfStreamFrame(buf.toList)
    buf.clear()
    frame

  private final def graphsToFrame[TNode, TTriple](
    buf: ListBuffer[RdfStreamRow], enc: ProtoEncoder[TNode, TTriple, ?, ?], graphs: Iterable[(TNode, Iterable[TTriple])]
  ): RdfStreamFrame =
    // GRAPHS
    // Note: this implementation does not carry the graphs over frame boundaries.
    graphs.foreach(params => {
      val (g, triples) = params
      enc.startGraph(g)
      triples.foreach(enc.addTripleStatement)
      enc.endGraph()
    })
    val frame = RdfStreamFrame(buf.toList)
    buf.clear()
    frame

  private final def quadsToFrame[T](
    buf: ListBuffer[RdfStreamRow], enc: ProtoEncoder[?, ?, T, ?], quads: Iterable[T]
  ): RdfStreamFrame =
    // QUADS
    quads.foreach(enc.addQuadStatement)
    val frame = RdfStreamFrame(buf.toList)
    buf.clear()
    frame
  
  protected final def serJelly(opt: RdfStreamOptions, closure: RdfStreamFrame => Unit): Unit =
    val buffer = ListBuffer.empty[RdfStreamRow]
    val encoder = JenaConverterFactory.encoder(ProtoEncoder.Params(
      opt, false, Some(buffer)
    ))
    sourceData match
      case Left(models) =>
        models.map(m => triplesToFrame(buffer, encoder, m.asTriples)).foreach(closure)
      case Right(datasets) =>
        if opt.physicalType.isGraphs then
          datasets.map(ds => graphsToFrame(buffer, encoder, ds.asGraphs)).foreach(closure)
        else
          datasets.map(ds => quadsToFrame(buffer, encoder, ds.asQuads)).foreach(closure)

  protected final def serJellyRdf4j(opt: RdfStreamOptions, closure: RdfStreamFrame => Unit): Unit =
    val buffer = ListBuffer.empty[RdfStreamRow]
    val encoder = Rdf4jConverterFactory.encoder(ProtoEncoder.Params(
      opt, false, Some(buffer)
    ))
    if opt.physicalType.isTriples then
      sourceDataRdf4j.map(triples => triplesToFrame(buffer, encoder, triples)).foreach(closure)
    else if opt.physicalType.isQuads then
      sourceDataRdf4j.map(quads => quadsToFrame(buffer, encoder, quads)).foreach(closure)
    // else graphs -- not supported for RDF4J in the benchmark

  protected final def serJellyOneElement(
    element: Model | DatasetGraph, encoder: JenaProtoEncoder, closure: RdfStreamFrame => Unit
  ): Unit =
    val buffer = encoder.maybeRowBuffer.get.asInstanceOf[ListBuffer[RdfStreamRow]]
    element match
      case m: Model => closure(triplesToFrame(buffer, encoder, m.asTriples))
      case ds: DatasetGraph =>
        if encoder.options.physicalType.isGraphs then
          closure(graphsToFrame(buffer, encoder, ds.asGraphs))
        else
          closure(quadsToFrame(buffer, encoder, ds.asQuads))

  protected final def serJena(sourceData: Model | DatasetGraph, format: RDFFormat, outputStream: OutputStream): Unit =
    val writer = RDFWriter.create().format(format)
    sourceData match
      case model: Model => writer.source(model.getGraph)
      case dataset: DatasetGraph => writer.source(dataset)
    writer.output(outputStream)
    
  protected final def serRdf4j(sourceData: Seq[Statement], format: rio.RDFFormat, outputStream: OutputStream): Unit =
    val writer = rio.Rio.createWriter(format, outputStream)
    writer.startRDF()
    sourceData.foreach(writer.handleStatement)
    writer.endRDF()

  protected final def desJelly(input: Iterable[Array[Byte]], streamType: String): Unit =
    desJellyInner(JenaConverterFactory, input, streamType)

  protected final def desJellyRdf4j(input: Iterable[Array[Byte]], streamType: String): Unit =
    desJellyInner(Rdf4jConverterFactory, input, streamType)

  private def desJellyInner(factory: ConverterFactory[?, ?, ?, ?, ?, ?], input: Iterable[Array[Byte]], streamType: String):
  Unit =
    val decoder = streamType match
      case "triples" => factory.triplesDecoder(None)
      case "quads" => factory.quadsDecoder(None)
      case "graphs" => factory.graphsDecoder(None)
    input
      .map(RdfStreamFrame.parseFrom)
      .foreach(frame => frame.rows.foreach(decoder.ingestRowFlat))
