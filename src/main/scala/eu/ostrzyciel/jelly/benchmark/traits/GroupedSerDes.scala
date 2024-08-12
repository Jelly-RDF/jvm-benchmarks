package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph

import java.io.OutputStream

trait GroupedSerDes extends SerDes:
  import eu.ostrzyciel.jelly.convert.jena.{*, given}

  protected var numElements: Long = _
  protected var numStatements: Long = _
  protected var sourceData: GroupedData = _

  protected final def loadData(path: String, streamType: String, elementSize: Int, elements: Option[Int]): Unit =
    val d = DataLoader.getSourceData(path, streamType, elementSize, elements)
    numStatements = d._1
    numElements = d._2
    sourceData = d._3
  
  protected final def serJelly(
    sourceData: GroupedData, opt: RdfStreamOptions, closure: RdfStreamFrame => Unit
  ): Unit =
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

  protected final def desJelly(input: Iterable[Array[Byte]], streamType: String): Unit =
    val decoder = streamType match
      case "triples" => JenaConverterFactory.triplesDecoder(None)
      case "quads" => JenaConverterFactory.quadsDecoder(None)
      case "graphs" => JenaConverterFactory.graphsDecoder(None)
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})
