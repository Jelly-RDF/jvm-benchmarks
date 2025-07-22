package eu.neverblink.jelly.benchmark.rdf

//package eu.neverblink.jelly.benchmark
//
//import eu.neverblink.jelly.benchmark.traits.GroupedSerDes
//import eu.neverblink.jelly.benchmark.util.*
//import org.apache.jena.rdf.model.Model
//import org.apache.jena.sparql.core.DatasetGraph
//
//import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
//import scala.collection.mutable.ArrayBuffer
//
//object GroupedSerDesBench extends GroupedSerDes:
//  import eu.neverblink.jelly.benchmark.util.Experiments.*
//  import eu.neverblink.jelly.benchmark.util.Util.*
//
//  /**
//   * @param tasks "ser", "des", or "ser,des"
//   * @param streamType "triples", "quads", "graphs"
//   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
//   * @param elements number of elements to process or 0 to process all
//   * @param sourceFilePath path to the source file
//   */
//  @main
//  def runGroupedSerDesBench(tasks: String, streamType: String, elementSize: Int, elements: Int, sourceFilePath: String):
//  Unit =
//    val taskSeq = tasks.split(',')
//    loadData(sourceFilePath, streamType, elementSize, if elements == 0 then None else Some(elements))
//
//    def saveResults(task: String): Unit =
//      saveRunInfo(s"grouped_raw_$task", Map(
//        "order" -> experiments,
//        "times" -> times,
//        "file" -> sourceFilePath,
//        "task" -> task,
//        "streamType" -> streamType,
//        "elementSize" -> elementSize,
//        "elements" -> numElements,
//        "statements" -> numStatements,
//      ))
//
//    if taskSeq.contains("ser") then
//      initExperiment(flatStreaming = false, jena = true, rdf4j = false, streamType)
//      mainSer()
//      saveResults("ser")
//      System.gc()
//    if taskSeq.contains("des") then
//      initExperiment(flatStreaming = false, jena = true, rdf4j = false, streamType)
//      mainDes()
//      saveResults("des")
//
//    sys.exit()
//
//
//  private def mainSer(): Unit =
//    for
//      experiment <- experiments
//      i <- 1 to ConfigManager.benchmarkRepeats
//    do
//      System.gc()
//      println("Sleeping 3 seconds...")
//      Thread.sleep(3000)
//      println(f"Try: $i, experiment: $experiment")
//      if experiment.startsWith("jelly") then
//        val stream = OutputStream.nullOutputStream
//        times(experiment) += time {
//          serJelly(getJellyOpts(experiment, streamType, grouped = true), frame => frame.writeTo(stream))
//        }
//      else
//        try {
//          val sourceFlat = sourceData match
//            case Left(v) => v
//            case Right(v) => v
//
//          times(experiment) += time {
//            for item <- sourceFlat do
//              serJena(item, getJenaFormat(experiment, streamType).get, OutputStream.nullOutputStream)
//          }
//        } catch {
//          case e: Exception =>
//            println(f"Failed to serialize with $experiment")
//            e.printStackTrace()
//        }
//
//  private def mainDes(): Unit =
//    for experiment <- experiments do
//      println("Serializing to memory...")
//      try {
//        val serialized = {
//          if experiment.startsWith("jelly") then
//            val serBuffer = ArrayBuffer[Array[Byte]]()
//            serJelly(
//              getJellyOpts(experiment, streamType, true),
//              frame => serBuffer.append(frame.toByteArray)
//            )
//            serBuffer
//          else
//            val jenaFormat = getJenaFormat(experiment, streamType).get
//            val serBuffer = ArrayBuffer[Array[Byte]]()
//            val sourceFlat = sourceData match
//              case Left(v) => v
//              case Right(v) => v
//            for item <- sourceFlat do
//              val oStream = new ByteArrayOutputStream()
//              serJena(item, jenaFormat, oStream)
//              serBuffer.append(oStream.toByteArray)
//            serBuffer
//        }
//
//        for i <- 1 to ConfigManager.benchmarkRepeats do
//          System.gc()
//          println("Sleeping 3 seconds...")
//          Thread.sleep(3000)
//          println(f"Try: $i, experiment: $experiment")
//          if experiment.startsWith("jelly") then
//            times(experiment) += time {
//              desJelly(serialized, streamType)
//            }
//          else
//            val jenaFormat = getJenaFormat(experiment, streamType).get
//            times(experiment) += time {
//              for buffer <- serialized do
//                desJena(
//                  new ByteArrayInputStream(buffer),
//                  jenaFormat,
//                  streamType
//                )
//            }
//      } catch {
//        case e: Exception =>
//          println(f"Failed experiment with $experiment")
//          e.printStackTrace()
//      }
//
