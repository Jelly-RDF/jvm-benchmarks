package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{InputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.Random

trait SerDesBench:
  protected final type StreamSeq = Either[Iterable[Model], Iterable[DatasetGraph]]

  protected val conf = ConfigFactory.load()

  implicit protected val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SerDesBench", conf)
  implicit protected val ec: ExecutionContext = system.executionContext

  protected val experiments = Random.shuffle(jenaFormats.keys ++ jellyOptions.keys)
  protected val times: Map[String, mutable.ArrayBuffer[Long]] = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap
  protected var numStatements: Long = 0
  protected var numElements: Long = 0

  protected final def serJelly(
    sourceData: StreamSeq, opt: RdfStreamOptions, closure: RdfStreamFrame => Unit
  ): Unit =
    val encoder = JenaConverterFactory.encoder(opt)
    sourceData match
      case Left(models) =>
        // TRIPLES
        models.map(m => {
            val rows = m.getGraph.find().asScala
              .flatMap(triple => encoder.addTripleStatement(triple))
              .toSeq
            RdfStreamFrame(rows)
          })
          .foreach(closure)
      case Right(datasets) =>
        if opt.streamType.isRdfStreamTypeGraphs then
          // GRAPHS
          // Note: this implementation does not carry the graphs over frame boundaries.
          datasets.map(ds => {
            val default = ds.getDefaultGraph
            val graphs = if !default.isEmpty then
              (null, default) :: ds.listGraphNodes().asScala.map(g => (g, ds.getGraph(g))).toList
            else ds.listGraphNodes().asScala.map(g => (g, ds.getGraph(g))).toList

            val rows = graphs.flatMap(params => {
              val (g, graph) = params
              encoder.startGraph(g) ++
                graph.find().asScala.flatMap(triple => encoder.addTripleStatement(triple)) ++
                encoder.endGraph()
            })
            RdfStreamFrame(rows)
          })
          .foreach(closure)
        else
          // QUADS
          datasets.map(m => {
              val rows = m.find().asScala
                .flatMap(quad => encoder.addQuadStatement(quad))
                .toSeq
              RdfStreamFrame(rows)
            })
            .foreach(closure)
  protected final def serJena(sourceData: Model | DatasetGraph, format: RDFFormat, outputStream: OutputStream): Unit =
    val writer = RDFWriter.create().format(format)
    sourceData match
      case model: Model => writer.source(model.getGraph)
      case dataset: DatasetGraph => writer.source(dataset)
    writer.output(outputStream)

  protected final def desJelly(input: Iterable[Array[Byte]], quads: Boolean): Unit =
    val decoder = if quads then JenaConverterFactory.quadsDecoder
    else JenaConverterFactory.triplesDecoder
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})

  protected final def desJena(input: InputStream, format: RDFFormat, useQuads: Boolean): Unit =
    (
      if !useQuads then AsyncParser.asyncParseTriples(input, format.getLang, "")
      else AsyncParser.asyncParseQuads(input, format.getLang, "")
    ).forEachRemaining(_ => {})
