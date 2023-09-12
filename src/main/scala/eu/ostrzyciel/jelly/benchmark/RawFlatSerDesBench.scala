package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions, RdfStreamType}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

object RawFlatSerDesBench:
  import Util.*

  private val conf = ConfigFactory.load()

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "RawFlatSerDesBench", conf)
  implicit val ec: ExecutionContext = system.executionContext

  private var useQuads = false
  private val experiments = jenaFormats.keys ++ jellyOptions.keys
  private val times: Map[String, mutable.ArrayBuffer[Long]] = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap
  private var datasetSize: Long = 0

  // Arguments: [ser/des] [source file path]
  def main(args: Array[String]): Unit =
    if args(1).contains(".nq") then useQuads = true

    if args(0) == "ser" then
      mainSer(args(1))
    else if args(0) == "des" then
      mainDes(args(1))

    printSpeed(datasetSize, times)
    saveRunInfo(s"raw_${args(0)}", conf, Map(
      "size" -> datasetSize,
      "times" -> times,
      "file" -> args(1),
      "task" -> args(0),
    ))
    sys.exit()

  private def getFormat(exp: String): RDFFormat =
    val tuple = jenaFormats(exp)
    if useQuads then tuple(1) else tuple(0)

  private def getJellyOpts(exp: String): RdfStreamOptions =
    jellyOptions(exp).withStreamType(
      if useQuads then RdfStreamType.RDF_STREAM_TYPE_QUADS
      else RdfStreamType.RDF_STREAM_TYPE_TRIPLES
    )

  private def getSourceFlat(path: String): Seq[Model | DatasetGraph] =
    println("Loading the source file...")
    if useQuads then
      val items = AsyncParser.asyncParseQuads(path).asScala
        .grouped(1000)
        .map(ts => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          ts.foreach(dataset.add)
          dataset
        }).toSeq
      datasetSize = items.map(_.size()).sum
      items
    else
      val items = AsyncParser.asyncParseTriples(path).asScala
        .grouped(1000)
        .map(ts => {
          val model = ModelFactory.createDefaultModel()
          ts.foreach(model.getGraph.add)
          model
      }).toSeq
      datasetSize = items.map(_.size()).sum
      items

  private def mainSer(path: String): Unit =
    val sourceData = getSourceFlat(path)

    for i <- 1 to REPEATS; experiment <- experiments do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Try: $i, experiment: $experiment")
      if experiment.startsWith("jelly") then
        val stream = OutputStream.nullOutputStream
        times(experiment) += time {
          serJelly(sourceData, getJellyOpts(experiment), frame => frame.writeTo(stream))
        }
      else
        times(experiment) += time {
          for item <- sourceData do
            serJena(item, getFormat(experiment), OutputStream.nullOutputStream)
        }

  private def mainDes(path: String): Unit =
    val source = getSourceFlat(path)

    for experiment <- experiments do
      println("Serializing to memory...")
      val serialized = {
        if experiment.startsWith("jelly") then
          val serBuffer = ArrayBuffer[Array[Byte]]()
          serJelly(source, getJellyOpts(experiment), frame => serBuffer.append(frame.toByteArray))
          serBuffer
        else
          val serBuffer = ArrayBuffer[Array[Byte]]()
          for model <- source do
            val oStream = new ByteArrayOutputStream()
            serJena(model, getFormat(experiment), oStream)
            serBuffer.append(oStream.toByteArray)
          serBuffer
      }

      for i <- 1 to REPEATS do
        System.gc()
        println("Sleeping 3 seconds...")
        Thread.sleep(3000)
        println(f"Try: $i, experiment: $experiment")
        if experiment.startsWith("jelly") then
          times(experiment) += time {
            desJelly(serialized, useQuads)
          }
        else
          times(experiment) += time {
            for buffer <- serialized do
              desJena(new ByteArrayInputStream(buffer), getFormat(experiment))
          }

  private def serJelly(
    sourceData: Seq[Model | DatasetGraph], opt: RdfStreamOptions, closure: RdfStreamFrame => Unit
  ): Unit =
    val encoder = JenaConverterFactory.encoder(opt)
    if !useQuads then
      sourceData.asInstanceOf[Seq[Model]]
        .map(m => {
          val rows = m.getGraph.find().asScala
            .flatMap(triple => encoder.addTripleStatement(triple))
            .toSeq
          RdfStreamFrame(rows)
        })
        .foreach(closure)
    else
      sourceData.asInstanceOf[Seq[DatasetGraph]]
        .map(m => {
          val rows = m.find().asScala
            .flatMap(quad => encoder.addQuadStatement(quad))
            .toSeq
          RdfStreamFrame(rows)
        })
        .foreach(closure)

  private def serJena(sourceData: Model | DatasetGraph, format: RDFFormat, outputStream: OutputStream): Unit =
    val writer = RDFWriter.create().format(format)
    sourceData match
      case model: Model => writer.source(model.getGraph)
      case dataset: DatasetGraph => writer.source(dataset)
    writer.output(outputStream)

  private def desJelly(input: Iterable[Array[Byte]], quads: Boolean): Unit =
    val decoder = if quads then JenaConverterFactory.quadsDecoder
    else JenaConverterFactory.triplesDecoder
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})

  private def desJena(input: InputStream, format: RDFFormat): Unit =
    (
      if !useQuads then AsyncParser.asyncParseTriples(input, format.getLang, "")
      else AsyncParser.asyncParseQuads(input, format.getLang, "")
    ).forEachRemaining(_ => {})
