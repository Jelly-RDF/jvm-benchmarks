package eu.neverblink.jelly.benchmark

import eu.neverblink.jelly.benchmark.traits.FlatSerDes
import eu.neverblink.jelly.benchmark.util.{ConfigManager, Experiments}
import eu.ostrzyciel.jelly.convert.rdf4j.rio.{JELLY, JellyWriterSettings}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.eclipse.rdf4j.rio.WriterConfig

import java.io.OutputStream

object FlatSerDesRdf4jBench extends FlatSerDes:
  import eu.neverblink.jelly.benchmark.util.Experiments.*
  import eu.neverblink.jelly.benchmark.util.Util.*

  /**
   * Benchmark for serializing and deserializing flat RDF streams using RDF4J.
   * Here we test the DELIMITED variant of Jelly and RDF4j's streaming RDF serializers.
   *
   * @param tasks "ser", "des", or "ser,des"
   * @param streamType "triples", "quads"
   * @param jellyFrameSize size of Jelly's RdfStreamFrames in rows
   * @param statements number of statements to process or 0 to process all
   * @param sourceFilePath path to the source file (only jelly.gz files are supported)
   */
  @main
  def runFlatSerDesRdf4jBench(
    tasks: String, streamType: String, jellyFrameSize: Int, statements: Int, sourceFilePath: String
  ): Unit =
    val taskSeq = tasks.split(',')
    loadDataRdf4j(sourceFilePath, statements)

    def saveResults(task: String): Unit =
      saveRunInfo(s"flat_raw_rdf4j_$task", Map(
        "statements" -> numStatements,
        "order" -> experiments,
        "times" -> times,
        "file" -> sourceFilePath,
        "task" -> task,
        "streamType" -> streamType,
      ))

    if taskSeq.contains("ser") then
      initExperiment(flatStreaming = true, jena = false, rdf4j = true, streamType)
      mainSer(jellyFrameSize)
      saveResults("ser")
      System.gc()
    if taskSeq.contains("des") then
      initExperiment(flatStreaming = true, jena = false, rdf4j = true, streamType)
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
      val (format, config) = if experiment.startsWith("jelly") then (
        JELLY,
        JellyWriterSettings.configFromOptions(
          getJellyOpts(experiment, streamType, grouped = false),
          jellyFrameSize
        )
      ) else (
        getRdf4jFormat(experiment, streamType).get,
        new WriterConfig()
      )
      
      try {
        times(experiment) += time {
          serRdf4j(format, config, outputStream)
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
        val (format, config) = if experiment.startsWith("jelly") then (
          JELLY,
          JellyWriterSettings.configFromOptions(
            getJellyOpts(experiment, streamType, grouped = false),
            jellyFrameSize
          )
        ) else (
          getRdf4jFormat(experiment, streamType).get,
          new WriterConfig()
        )
        serRdf4j(format, config, outputStream)

        if outputStream.size() <= 0 then
          throw new Exception("Serialization failed -- buffer size is larger than Int.MaxValue")

        for i <- 1 to ConfigManager.benchmarkRepeats do
          System.gc()
          println("Sleeping 3 seconds...")
          Thread.sleep(3000)
          println(f"Try: $i, experiment: $experiment")
          if experiment.startsWith("jelly") then
            times(experiment) += time {
              desRdf4j(outputStream.toInputStream, JELLY)
            }
          else
            times(experiment) += time {
              desRdf4j(
                outputStream.toInputStream,
                getRdf4jFormat(experiment, streamType).get
              )
            }
      } catch {
        case e: Exception =>
          println(f"Failed experiment with $experiment")
          e.printStackTrace()
      }
