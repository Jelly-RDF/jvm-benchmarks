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

  /**
   * @param tasks "ser", "des", or "ser,des"
   * @param streamType "triples", "quads", "graphs"
   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
   * @param sourceFilePath path to the source file
   */
  @main
  def main(tasks: String, streamType: String, elementSize: Int, sourceFilePath: String): Unit =
    val taskSeq = tasks.split(',')
    loadData(sourceFilePath, streamType, elementSize)

    def saveResults(task: String): Unit =
      saveRunInfo(s"stream_raw_$task", Map(
        "order" -> experiments,
        "times" -> times,
        "file" -> sourceFilePath,
        "task" -> task,
        "streamType" -> streamType,
        "elementSize" -> elementSize,
        "elements" -> numElements,
        "statements" -> numStatements,
      ))

    if taskSeq.contains("ser") then
      initExperiment(flatStreaming = false, streamType)
      mainSer()
      saveResults("ser")
      System.gc()
    if taskSeq.contains("des") then
      initExperiment(flatStreaming = false, streamType)
      mainDes()
      saveResults("des")
    
    sys.exit()
    

  private def mainSer(): Unit =
    for i <- 1 to ConfigManager.benchmarkRepeats; experiment <- experiments do
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
              serJena(item, getJenaFormat(experiment, streamType).get, OutputStream.nullOutputStream)
          }
        } catch {
          case e: Exception =>
            println(f"Failed to serialize with $experiment")
            e.printStackTrace()
        }

  private def mainDes(): Unit =
    for experiment <- experiments do
      println("Serializing to memory...")
      try {
        val serialized = {
          if experiment.startsWith("jelly") then
            val serBuffer = ArrayBuffer[Array[Byte]]()
            serJelly(
              sourceData, 
              getJellyOpts(experiment, streamType, true),
              frame => serBuffer.append(frame.toByteArray)
            )
            serBuffer
          else
            val jenaFormat = getJenaFormat(experiment, streamType).get
            val serBuffer = ArrayBuffer[Array[Byte]]()
            val sourceFlat = sourceData match
              case Left(v) => v
              case Right(v) => v
            for item <- sourceFlat do
              val oStream = new ByteArrayOutputStream()
              serJena(item, jenaFormat, oStream)
              serBuffer.append(oStream.toByteArray)
            serBuffer
        }

        for i <- 1 to ConfigManager.benchmarkRepeats do
          System.gc()
          println("Sleeping 3 seconds...")
          Thread.sleep(3000)
          println(f"Try: $i, experiment: $experiment")
          if experiment.startsWith("jelly") then
            times(experiment) += time {
              desJelly(serialized, streamType)
            }
          else
            val jenaFormat = getJenaFormat(experiment, streamType).get
            times(experiment) += time {
              for buffer <- serialized do
                desJena(
                  new ByteArrayInputStream(buffer),
                  jenaFormat, 
                  streamType
                )
            }
      } catch {
        case e: Exception =>
          println(f"Failed experiment with $experiment")
          e.printStackTrace()
      }

