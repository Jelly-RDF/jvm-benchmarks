package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.traits.GroupedSerDes
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph

import java.io.{ByteArrayOutputStream, OutputStream}
import scala.collection.mutable

object SizeBench extends GroupedSerDes:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*

  private val sizes: mutable.Map[String, Long] = mutable.Map.empty

  /**
   * @param streamType "triples", "quads", "graphs"
   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
   * @param elements number of elements to process or 0 to process all
   * @param sourceFilePath path to the source file
   */
  @main
  def runSizeBench(streamType: String, elementSize: Int, elements: Int, sourceFilePath: String): Unit =
    initExperiment(flatStreaming = false, streamType)
    loadData(sourceFilePath, streamType, elementSize, if elements == 0 then None else Some(elements))
    run()
    saveRunInfo(s"size_$streamType", Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "sizes" -> sizes,
      "file" -> sourceFilePath,
      "elementSize" -> elementSize,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def run(): Unit =
    for gzip <- Seq(false, true); experiment <- experiments do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Running experiment $experiment with gzip $gzip")

      val expName = if gzip then s"$experiment-gzip" else experiment
      sizes.getOrElseUpdate(expName, 0L)

      def getOs: (OutputStream, ByteArrayOutputStream) =
        val baos = new ByteArrayOutputStream()
        if gzip then
          (new java.util.zip.GZIPOutputStream(baos), baos)
        else (baos, baos)

      try {
        if experiment.startsWith("jelly") then
          var i = 0L
          serJelly(sourceData, getJellyOpts(experiment, streamType, grouped = false), frame => {
            val (os, baos) = getOs
            frame.writeTo(os)
            os.close()
            sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))
            i += 1
          })
        else
          val sourceFlat = sourceData match
            case Left(v) => v
            case Right(v) => v
          for item <- sourceFlat do
            val (os, baos) = getOs
            serJena(item, getJenaFormat(experiment, streamType).get, os)
            os.close()
            sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))
      }
      catch
        case e: Exception =>
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
          System.gc()
          Thread.sleep(1000)
