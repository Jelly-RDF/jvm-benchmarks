package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamOptions, RdfStreamType}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.Random

object FlatSerDesBench extends SerDesBench:
  import Util.*

  private val conf = ConfigFactory.load()

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "FlatSerDesBench", conf)
  implicit val ec: ExecutionContext = system.executionContext

  private var useQuads = false
  private val experiments = Random.shuffle(jenaFormats.keys ++ jellyOptions.keys)
  private val times: Map[String, mutable.ArrayBuffer[Long]] = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap
  private var numStatements: Long = 0
  private var numElements: Long = 0

  // Arguments: [ser/des] [source file path]
  def main(args: Array[String]): Unit =
    if args(1).contains(".nq") then useQuads = true

    if args(0) == "ser" then
      mainSer(args(1))
    else if args(0) == "des" then
      mainDes(args(1))

    printSpeed(numStatements, times)
    saveRunInfo(s"flat_raw_${args(0)}", conf, Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
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

  private def getSourceFlat(path: String): Either[Seq[Model], Seq[DatasetGraph]] =
    println("Loading the source file...")
    if useQuads then
      val items = AsyncParser.asyncParseQuads(path).asScala
        .grouped(1000)
        .map(ts => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          ts.foreach(dataset.add)
          dataset
        }).toSeq
      numStatements = items.map(_.size()).sum
      numElements = items.size
      Right(items)
    else
      val items = AsyncParser.asyncParseTriples(path).asScala
        .grouped(1000)
        .map(ts => {
          val model = ModelFactory.createDefaultModel()
          ts.foreach(model.getGraph.add)
          model
      }).toSeq
      numStatements = items.map(_.size()).sum
      numElements = items.size
      Left(items)

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
        try {
          val sourceFlat = sourceData match
            case Left(v) => v
            case Right(v) => v

          times(experiment) += time {
            for item <- sourceFlat do
              serJena(item, getFormat(experiment), OutputStream.nullOutputStream)
          }
        } catch {
          case e: Exception =>
            println(f"Failed to serialize with $experiment")
            println(e)
        }

  private def mainDes(path: String): Unit =
    val source = getSourceFlat(path)

    for experiment <- experiments do
      println("Serializing to memory...")
      try {
        val serialized = {
          if experiment.startsWith("jelly") then
            val serBuffer = ArrayBuffer[Array[Byte]]()
            serJelly(source, getJellyOpts(experiment), frame => serBuffer.append(frame.toByteArray))
            serBuffer
          else
            val serBuffer = ArrayBuffer[Array[Byte]]()
            val sourceFlat = source match
              case Left(v) => v
              case Right(v) => v
            for item <- sourceFlat do
              val oStream = new ByteArrayOutputStream()
              serJena(item, getFormat(experiment), oStream)
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
                desJena(new ByteArrayInputStream(buffer), getFormat(experiment), useQuads)
            }
      } catch {
        case _: Exception => println(f"Failed experiment with $experiment")
      }
