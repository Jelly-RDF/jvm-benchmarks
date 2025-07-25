package eu.neverblink.jelly.benchmark.rdf

import eu.neverblink.jelly.benchmark.rdf.traits.FlatSerDes
import eu.neverblink.jelly.benchmark.rdf.util.Experiments
import eu.neverblink.jelly.benchmark.util.ConfigManager
import eu.neverblink.jelly.convert.jena.riot.*
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.riot.RIOT

import java.io.OutputStream

object FlatSerDesBench extends FlatSerDes:
  import Experiments.*
  import eu.neverblink.jelly.benchmark.util.Util.*

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
      initExperiment(flatStreaming = true, jena = true, rdf4j = false, streamType)
      mainSer(jellyFrameSize)
      saveResults("ser")
      System.gc()
    if taskSeq.contains("des") then
      initExperiment(flatStreaming = true, jena = true, rdf4j = false, streamType)
      mainDes(jellyFrameSize)
      saveResults("des")

    sys.exit()


  private def mainSer(jellyFrameSize: Int): Unit =
    for experiment <- experiments; i <- 1 to ConfigManager.benchmarkRepeats do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Try: $i, experiment: $experiment")
      val outputStream = OutputStream.nullOutputStream
      val context = RIOT.getContext.copy()
      val format = if experiment.startsWith("jelly") then
        val opts = getJellyOpts(experiment, streamType, grouped = false)
        context.set(JellyLanguage.SYMBOL_STREAM_OPTIONS, opts)
          .set(JellyLanguage.SYMBOL_FRAME_SIZE, 512)
        JellyFormat.JELLY_SMALL_STRICT // this gets overridden by the context
      else
        getJenaFormat(experiment, streamType).get

      try {
        times(experiment) += time {
          serJena(format, context, outputStream)
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
        // Use Apache Commons implementation of ByteArrayOutputStream to avoid additional buffer copying.
        // Note that this implementation is still limited to Int.MaxValue bytes.
        // This would fail for example with jena-nt, 10M quads, assist-iot-weather-graphs.
        // It's safer to limit the number of statements to 5M.
        val outputStream = new ByteArrayOutputStream()
        val context = RIOT.getContext.copy()
        val format = if experiment.startsWith("jelly") then
          val opts = getJellyOpts(experiment, streamType, grouped = false)
          context.set(JellyLanguage.SYMBOL_STREAM_OPTIONS, opts)
            .set(JellyLanguage.SYMBOL_FRAME_SIZE, 512)
          JellyFormat.JELLY_SMALL_STRICT // this gets overridden by the context
        else
          getJenaFormat(experiment, streamType).get
        serJena(format, context, outputStream)

        if outputStream.size() <= 0 then
          throw new Exception("Serialization failed -- buffer size is larger than Int.MaxValue")

        for i <- 1 to ConfigManager.benchmarkRepeats do
          System.gc()
          println("Sleeping 3 seconds...")
          Thread.sleep(3000)
          println(f"Try: $i, experiment: $experiment")
          if experiment.startsWith("jelly") then
            times(experiment) += time {
              desJena(outputStream.toInputStream, JellyFormat.JELLY_BIG_ALL_FEATURES, streamType)
            }
          else
            times(experiment) += time {
              desJena(
                outputStream.toInputStream,
                getJenaFormat(experiment, streamType).get,
                streamType
              )
            }
      } catch {
        case e: Exception =>
          println(f"Failed experiment with $experiment")
          e.printStackTrace()
      }
