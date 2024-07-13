package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{InputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.Random

trait SerDesBench:
  import eu.ostrzyciel.jelly.convert.jena.{given, *}

  implicit protected val system: ActorSystem[Nothing] = ActorSystem(
    Behaviors.empty, "SerDesBench", ConfigManager.config
  )
  implicit protected val ec: ExecutionContext = system.executionContext

  protected var experiments: Seq[String] = _
  protected var times: Map[String, mutable.ArrayBuffer[Long]] = _
  protected var streamType: String = _
  protected var numElements: Long = _
  protected var numStatements: Long = _
  protected var sourceData: BenchmarkData = _
  
  protected final def loadData(path: String, streamType: String, elementSize: Int): Unit =
    val d = DataLoader.getSourceData(path, streamType, elementSize)
    numStatements = d._1
    numElements = d._2
    sourceData = d._3

  protected final def initExperiment(flatStreaming: Boolean, streamType: String): Unit =
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val doJena = streamType != "graphs"
    this.streamType = streamType
    experiments = Experiments.getFormatKeysToTest(
      jena = doJena && !flatStreaming, 
      jenaStreaming = doJena, 
      jelly = true, 
      streamType
    )
    times = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap

  protected final def serJelly(
    sourceData: BenchmarkData, opt: RdfStreamOptions, closure: RdfStreamFrame => Unit
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

  protected final def desJena(input: InputStream, format: RDFFormat, streamType: String): Unit =
    (
      if streamType == "triples" then AsyncParser.asyncParseTriples(input, format.getLang, "")
      else AsyncParser.asyncParseQuads(input, format.getLang, "")
    ).forEachRemaining(_ => {})
