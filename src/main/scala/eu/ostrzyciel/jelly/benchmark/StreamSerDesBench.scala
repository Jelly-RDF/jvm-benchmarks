package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.stream.scaladsl.Sink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, OutputStream}
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

object StreamSerDesBench extends SerDesBench:
  import Experiments.*
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  // Arguments: [ser/des] [triples/graphs/quads] [source file path]
  def main(args: Array[String]): Unit =
    val task = args(0)
    val streamType = args(1)
    val filePath = args(2)
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val experiments2 = (if streamType != "graphs" then experiments else Random.shuffle(jellyOptions.keys)).toSeq

    if task == "ser" then
      mainSer(filePath, streamType, experiments2)
    else if task == "des" then
      mainDes(filePath, streamType, experiments2)

    printSpeed(numStatements, times)
    saveRunInfo(s"stream_raw_${args(0)}", conf, Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "times" -> times,
      "file" -> filePath,
      "task" -> task,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def mainSer(path: String, streamType: String, exps: Seq[String]): Unit =
    val (_, _, sourceData) = DataLoader.getSourceData(path, streamType, 0)

    for i <- 1 to REPEATS; experiment <- exps do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Try: $i, experiment: $experiment")
      if experiment.startsWith("jelly") then
        val stream = OutputStream.nullOutputStream
        times(experiment) += time {
          serJelly(sourceData, getJellyOpts(experiment, streamType, true), frame => frame.writeTo(stream))
        }
      else
        try {
          val sourceFlat = sourceData match
            case Left(v) => v
            case Right(v) => v

          times(experiment) += time {
            for item <- sourceFlat do
              serJena(item, getFormat(experiment, streamType), OutputStream.nullOutputStream)
          }
        } catch {
          case e: Exception =>
            println(f"Failed to serialize with $experiment")
            e.printStackTrace()
        }

  private def mainDes(path: String, streamType: String, exps: Seq[String]): Unit =
    val (_, _, source) = DataLoader.getSourceData(path, streamType, 0)

    for experiment <- exps do
      println("Serializing to memory...")
      try {
        val serialized = {
          if experiment.startsWith("jelly") then
            val serBuffer = ArrayBuffer[Array[Byte]]()
            serJelly(source, getJellyOpts(experiment, streamType, true), frame => serBuffer.append(frame.toByteArray))
            serBuffer
          else
            val serBuffer = ArrayBuffer[Array[Byte]]()
            val sourceFlat = source match
              case Left(v) => v
              case Right(v) => v
            for item <- sourceFlat do
              val oStream = new ByteArrayOutputStream()
              serJena(item, getFormat(experiment, streamType), oStream)
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
              desJelly(serialized, streamType)
            }
          else
            times(experiment) += time {
              for buffer <- serialized do
                desJena(new ByteArrayInputStream(buffer), getFormat(experiment, streamType), streamType != "triples")
            }
      } catch {
        case e: Exception =>
          println(f"Failed experiment with $experiment")
          e.printStackTrace()
      }

