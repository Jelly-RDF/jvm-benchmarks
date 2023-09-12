package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions, RdfStreamType}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

// TODO: quad support
object RawFlatSerDesBench:
  import Util.*

  private val conf = ConfigFactory.load()

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "RawFlatSerDesBench", conf)
  implicit val ec: ExecutionContext = system.executionContext

  private var useQuads = false
  private val experiments = jenaFormats.keys ++ jellyOptions.keys
  private val times: Map[String, mutable.ArrayBuffer[Long]] = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap
  private var modelSize: Long = 0

  // Arguments: [ser/des] [source file path]
  def main(args: Array[String]): Unit =
    if args(1).contains(".nq") then useQuads = true

    if args(0) == "ser" then
      mainSer(args(1))
    else if args(0) == "des" then
      mainDes(args(1))

    printSpeed(modelSize, times)
    saveRunInfo(s"raw_${args(0)}", conf, Map(
      "size" -> modelSize,
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

  private def getSourceModels(path: String): Seq[Model] =
    println("Loading the source file...")
    val models = AsyncParser.asyncParseTriples(path).asScala
      .grouped(1000)
      .map(ts => {
        val model = ModelFactory.createDefaultModel()
        ts.foreach(model.getGraph.add)
        model
      })
      .toSeq
    modelSize = models.map(_.size()).sum
    models

  private def mainSer(path: String): Unit =
    val sourceModels = getSourceModels(path)

    for i <- 1 to REPEATS; experiment <- experiments do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Try: $i, experiment: $experiment")
      if experiment.startsWith("jelly") then
        val stream = OutputStream.nullOutputStream
        times(experiment) += time {
          serJelly(sourceModels, getJellyOpts(experiment), frame => frame.writeTo(stream))
        }
      else
        times(experiment) += time {
          for model <- sourceModels do
            serJena(model, getFormat(experiment), OutputStream.nullOutputStream)
        }

  private def mainDes(path: String): Unit =
    val source = getSourceModels(path)

    for experiment <- experiments do
      println("Serializing to memory...")
      // TODO: check and report serialized sizes
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

  private def serJelly(sourceModels: Seq[Model], opt: RdfStreamOptions, closure: RdfStreamFrame => Unit): Unit =
    val encoder = JenaConverterFactory.encoder(opt)
    sourceModels
      .map(m => {
        val rows = m.getGraph.find().asScala
          .flatMap(triple => encoder.addTripleStatement(triple))
          .toSeq
        RdfStreamFrame(rows)
      })
      .foreach(closure)

  private def serJena(sourceModel: Model, format: RDFFormat, outputStream: OutputStream): Unit =
    RDFWriter.create()
      .format(format)
      .source(sourceModel.getGraph)
      .output(outputStream)

  private def desJelly(input: Iterable[Array[Byte]], quads: Boolean): Unit =
    val decoder = if quads then JenaConverterFactory.quadsDecoder
    else JenaConverterFactory.triplesDecoder
    input
      .map(RdfStreamFrame.parseFrom)
      .map(frame => frame.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})

  private def desJena(input: InputStream, format: RDFFormat): Unit =
    AsyncParser.asyncParseTriples(input, format.getLang, "")
      .forEachRemaining(_ => {})
