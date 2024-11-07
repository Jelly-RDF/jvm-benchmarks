package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.ConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio

import java.io.OutputStream

trait GroupedSerDes extends SerDes:
  import eu.ostrzyciel.jelly.convert.jena.{*, given}
  import eu.ostrzyciel.jelly.convert.rdf4j.Rdf4jConverterFactory

  protected var numElements: Long = _
  protected var numStatements: Long = _
  protected var numElementsRdf4j: Long = _
  protected var numStatementsRdf4j: Long = _
  protected var sourceData: GroupedData = _
  protected var sourceDataRdf4j: GroupedDataRdf4j = _

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
  
  protected final def serJelly(opt: RdfStreamOptions, closure: RdfStreamFrame => Unit): Unit =
    val encoder = JenaConverterFactory.encoder(opt)
    sourceData match
      case Left(models) =>
        // TRIPLES
        models.map(m => {
            val rows = m.asTriples
              .flatMap(triple => encoder.addTripleStatement(triple))
              .toSeq
            RdfStreamFrame(rows)
          })
          .foreach(closure)
      case Right(datasets) =>
        if opt.physicalType.isGraphs then
          // GRAPHS
          // Note: this implementation does not carry the graphs over frame boundaries.
          datasets.map(ds => {
              val rows = ds.asGraphs.flatMap(params => {
                val (g, triples) = params
                encoder.startGraph(g) ++
                  triples.flatMap(triple => encoder.addTripleStatement(triple)) ++
                  encoder.endGraph()
              }).toSeq
              RdfStreamFrame(rows)
            })
            .foreach(closure)
        else
          // QUADS
          datasets.map(ds => {
              val rows = ds.asQuads
                .flatMap(quad => encoder.addQuadStatement(quad))
                .toSeq
              RdfStreamFrame(rows)
            })
            .foreach(closure)

  protected final def serJellyRdf4j(opt: RdfStreamOptions, closure: RdfStreamFrame => Unit): Unit =
    val encoder = Rdf4jConverterFactory.encoder(opt)
    if opt.physicalType.isTriples then
      sourceDataRdf4j.map(triples => {
          val rows = triples.flatMap(triple => encoder.addTripleStatement(triple))
          RdfStreamFrame(rows)
        })
        .foreach(closure)
    else if opt.physicalType.isQuads then
      sourceDataRdf4j.map(quads => {
          val rows = quads.flatMap(quad => encoder.addQuadStatement(quad))
          RdfStreamFrame(rows)
        })
        .foreach(closure)
    // else graphs -- not supported for RDF4J in the benchmark

  protected final def serJellyOneElement(
    element: Model | DatasetGraph, encoder: JenaProtoEncoder, closure: RdfStreamFrame => Unit
  ): Unit =
    element match
      case m: Model =>
        // TRIPLES
        val rows = m.asTriples
          .flatMap(triple => encoder.addTripleStatement(triple))
          .toSeq
        closure(RdfStreamFrame(rows))
      case ds: DatasetGraph =>
        if encoder.options.physicalType.isGraphs then
          // GRAPHS
          val rows = ds.asGraphs.flatMap(params => {
            val (g, triples) = params
            encoder.startGraph(g) ++
              triples.flatMap(triple => encoder.addTripleStatement(triple)) ++
              encoder.endGraph()
          }).toSeq
          closure(RdfStreamFrame(rows))
        else
          // QUADS
          val rows = ds.asQuads
            .flatMap(quad => encoder.addQuadStatement(quad))
            .toSeq
          closure(RdfStreamFrame(rows))

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

  private def desJellyInner(factory: ConverterFactory[_, _, _, _, _, _], input: Iterable[Array[Byte]], streamType: String):
  Unit =
    val decoder = streamType match
      case "triples" => factory.triplesDecoder(None)
      case "quads" => factory.quadsDecoder(None)
      case "graphs" => factory.graphsDecoder(None)
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})
