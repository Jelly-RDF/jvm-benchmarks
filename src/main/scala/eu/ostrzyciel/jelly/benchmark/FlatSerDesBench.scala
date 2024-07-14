package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.traits.FlatSerDes
import eu.ostrzyciel.jelly.benchmark.util.{ConfigManager, Experiments}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FlatSerDesBench extends FlatSerDes:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*

  /**
   * Benchmark for serializing and deserializing flat RDF streams.
   * Here we test the DELIMITED variant of Jelly and Jena's statement-level streaming RDF serializers.
   *
   * @param tasks "ser", "des", or "ser,des"
   * @param streamType "triples", "quads"
   * @param jellyFrameSize size of Jelly's RdfStreamFrames in rows
   * @param statements number of statements to process or 0 to process all
   * @param sourceFilePath path to the source file (only jelly.gz files are supported)
   */
  @main
  def runFlatSerDesBench(
    tasks: String, streamType: String, jellyFrameSize: Int, statements: Int, sourceFilePath: String
  ): Unit =
    val taskSeq = tasks.split(',')
    loadData(sourceFilePath, streamType, statements)

    def saveResults(task: String): Unit =
      saveRunInfo(s"flat_raw_$task", Map(
        "statements" -> numStatements,
        "order" -> experiments,
        "times" -> times,
        "file" -> sourceFilePath,
        "task" -> task,
        "streamType" -> streamType,
      ))

    if taskSeq.contains("ser") then
      initExperiment(flatStreaming = false, streamType)
      mainSer(jellyFrameSize)
      saveResults("ser")
      System.gc()
    if taskSeq.contains("des") then
      initExperiment(flatStreaming = false, streamType)
      mainDes(jellyFrameSize)
      saveResults("des")

    sys.exit()


  private def mainSer(jellyFrameSize: Int): Unit =
    for i <- 1 to ConfigManager.benchmarkRepeats; experiment <- experiments do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Try: $i, experiment: $experiment")
      val outputStream = OutputStream.nullOutputStream

      if experiment.startsWith("jelly") then
        times(experiment) += time {
          serJelly(
            getJellyOpts(experiment, streamType, grouped = false),
            _.writeDelimitedTo(outputStream),
            jellyFrameSize
          )
        }
      else
        try {
          times(experiment) += time {
            serJena(getJenaFormat(experiment, streamType).get, outputStream)
          }
        } catch {
          case e: Exception =>
            println(f"Failed to serialize with $experiment")
            e.printStackTrace()
        }

  private def mainDes(jellyFrameSize: Int): Unit =
    for experiment <- experiments do
      println("Serializing to memory...")
      try {

        val serialized = if experiment.startsWith("jelly") then
          // Keep the buffer withing this code block to deallocate it after we finish serializing
          val outputStream = new ByteArrayOutputStream()
          serJelly(
            getJellyOpts(experiment, streamType, grouped = false),
            _.writeDelimitedTo(outputStream),
            jellyFrameSize,
          )
          outputStream.toByteArray
        else
          val outputStream = new ByteArrayOutputStream()
          serJena(getJenaFormat(experiment, streamType).get, outputStream)
          outputStream.toByteArray

        for i <- 1 to ConfigManager.benchmarkRepeats do
          System.gc()
          println("Sleeping 3 seconds...")
          Thread.sleep(3000)
          println(f"Try: $i, experiment: $experiment")
          if experiment.startsWith("jelly") then
            times(experiment) += time {
              desJelly(new ByteArrayInputStream(serialized), streamType)
            }
          else
            times(experiment) += time {
              desJena(
                new ByteArrayInputStream(serialized),
                getJenaFormat(experiment, streamType).get,
                streamType
              )
            }
      } catch {
        case e: Exception =>
          println(f"Failed experiment with $experiment")
          e.printStackTrace()
      }
