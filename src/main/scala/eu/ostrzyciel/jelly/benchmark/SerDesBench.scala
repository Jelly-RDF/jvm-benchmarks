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
  import eu.ostrzyciel.jelly.convert.jena.*

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
            val rows = m.asTriples
              .flatMap(triple => encoder.addTripleStatement(triple))
              .toSeq
            RdfStreamFrame(rows)
          })
          .foreach(closure)
      case Right(datasets) =>
        if opt.streamType.isGraphs then
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
  protected final def serJena(sourceData: Model | DatasetGraph, format: RDFFormat, outputStream: OutputStream): Unit =
    val writer = RDFWriter.create().format(format)
    sourceData match
      case model: Model => writer.source(model.getGraph)
      case dataset: DatasetGraph => writer.source(dataset)
    writer.output(outputStream)

  protected final def desJelly(input: Iterable[Array[Byte]], streamType: String): Unit =
    val decoder = streamType match
      case "triples" => JenaConverterFactory.triplesDecoder
      case "quads" => JenaConverterFactory.quadsDecoder
      case "graphs" => JenaConverterFactory.graphsDecoder
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})

  protected final def desJena(input: InputStream, format: RDFFormat, useQuads: Boolean): Unit =
    (
      if !useQuads then AsyncParser.asyncParseTriples(input, format.getLang, "")
      else AsyncParser.asyncParseQuads(input, format.getLang, "")
    ).forEachRemaining(_ => {})
