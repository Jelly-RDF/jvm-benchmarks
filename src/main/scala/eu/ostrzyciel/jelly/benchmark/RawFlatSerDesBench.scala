package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import eu.ostrzyciel.jelly.stream.JellyOptionsFromTypesafe
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

object RawFlatSerDesBench:
  import Util.*

  val conf = ConfigFactory.load()
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "RawSerDesBench", conf)
  implicit val ec: ExecutionContext = system.executionContext

  val times = mutable.ArrayBuffer.empty[Long]
  var modelSize: Long = 0

  // Arguments: [ser/des] [method] [source file path]
  def main(args: Array[String]): Unit =
    if args(0) == "ser" then
      mainSer(args)
    else if args(0) == "des" then
      mainDes(args)

    printSpeed(modelSize, times)
    saveRunInfo(s"raw_${args(0)}", conf, Map(
      "size" -> modelSize,
      "times" -> times,
      "method" -> args(1),
      "file" -> args(2),
      "task" -> args(0),
    ))
    sys.exit()

  private def getFormat(args: Array[String]): RDFFormat =
    val tuple = jenaFormats(args(1))
    if args(2).contains(".nt") then tuple(0) else tuple(1)

  private def getSourceModels(args: Array[String]): Seq[Model] =
    println("Loading the source file...")
    val models = AsyncParser.asyncParseTriples(args(2)).asScala
      .grouped(1000)
      .map(ts => {
        val model = ModelFactory.createDefaultModel()
        ts.foreach(model.getGraph.add)
        model
      })
      .toSeq
    modelSize = models.map(_.size()).sum
    models

  private def mainSer(args: Array[String]): Unit =
    val sourceModels = getSourceModels(args)

    for i <- 1 to REPEATS do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println("Try: " + i)
      if args(1) == "jelly" then
        val stream = OutputStream.nullOutputStream
        times += time {
          serJelly(sourceModels, frame => frame.writeTo(stream))
        }
      else
        times += time {
          for model <- sourceModels do
            serJena(model, getFormat(args), OutputStream.nullOutputStream)
        }

  private def mainDes(args: Array[String]): Unit =
    println("Serializing to memory...")

    if args(1) == "jelly" then
      val serialized = {
        val serBuffer = ArrayBuffer[Array[Byte]]()
        serJelly(getSourceModels(args), frame => serBuffer.append(frame.toByteArray))
        serBuffer
      }

      for i <- 1 to REPEATS do
        System.gc()
        println("Sleeping 3 seconds...")
        Thread.sleep(3000)
        println("Try: " + i)
        times += time {
          desJelly(serialized, args(2).contains(".nq"))
        }

    else
      val serBuffer = ArrayBuffer[Array[Byte]]()
      for model <- getSourceModels(args) do
        val oStream = new ByteArrayOutputStream()
        serJena(model, getFormat(args), oStream)
        serBuffer.append(oStream.toByteArray)

      for i <- 1 to REPEATS do
        System.gc()
        println("Sleeping 3 seconds...")
        Thread.sleep(3000)
        println("Try: " + i)
        times += time {
          for buffer <- serBuffer do
            desJena(new ByteArrayInputStream(buffer), getFormat(args))
        }


  private def serJelly(sourceModels: Seq[Model], closure: RdfStreamFrame => Unit): Unit =
    val encoder = JenaConverterFactory.encoder(
      JellyOptionsFromTypesafe.fromTypesafeConfig(conf)
    )
    sourceModels
      .map(m => {
        val rows = m.getGraph.stream().iterator.asScala
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
      .map(stream => stream.rows.map(decoder.ingestRow).foreach(_ => {}))
      .foreach(_ => {})

  private def desJena(input: InputStream, format: RDFFormat): Unit =
    AsyncParser.asyncParseTriples(input, format.getLang, "")
      .forEachRemaining(_ => {})
