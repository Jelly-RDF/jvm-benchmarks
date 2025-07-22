//package eu.neverblink.jelly.benchmark
//
//import eu.neverblink.jelly.benchmark.traits.GroupedSerDes
//import eu.neverblink.jelly.benchmark.util.*
//
//import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
//import scala.collection.mutable.ArrayBuffer
//
//object GroupedSerDesRdf4jBench extends GroupedSerDes:
//  import eu.neverblink.jelly.benchmark.util.Experiments.*
//  import eu.neverblink.jelly.benchmark.util.Util.*
//
//  /**
//   * @param tasks "ser", "des", or "ser,des"
//   * @param streamType "triples" or "quads". "graphs" is not supported.
//   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
//   * @param elements number of elements to process or 0 to process all
//   * @param sourceFilePath path to the source file
//   */
//  @main
//  def runGroupedSerDesRdf4jBench(tasks: String, streamType: String, elementSize: Int, elements: Int, sourceFilePath: String):
//  Unit =
//    val taskSeq = tasks.split(',')
//    loadDataRdf4j(sourceFilePath, elementSize, if elements == 0 then None else Some(elements))
//
//    def saveResults(task: String): Unit =
//      saveRunInfo(s"grouped_raw_rdf4j_$task", Map(
//        "order" -> experiments,
//        "times" -> times,
//        "file" -> sourceFilePath,
//        "task" -> task,
//        "streamType" -> streamType,
//        "elementSize" -> elementSize,
//        "elements" -> numElementsRdf4j,
//        "statements" -> numStatementsRdf4j,
//      ))
//
//    if taskSeq.contains("ser") then
//      initExperiment(flatStreaming = false, jena = false, rdf4j = true, streamType)
//      mainSer()
//      saveResults("ser")
//      System.gc()
//    if taskSeq.contains("des") then
//      initExperiment(flatStreaming = false, jena = false, rdf4j = true, streamType)
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
//          serJellyRdf4j(getJellyOpts(experiment, streamType, grouped = true), frame => frame.writeTo(stream))
//        }
//      else
//        try {
//          times(experiment) += time {
//            for item <- sourceDataRdf4j do
//              serRdf4j(item, getRdf4jFormat(experiment, streamType).get, OutputStream.nullOutputStream)
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
//            serJellyRdf4j(
//              getJellyOpts(experiment, streamType, true),
//              frame => serBuffer.append(frame.toByteArray)
//            )
//            serBuffer
//          else
//            val rdf4jFormat = getRdf4jFormat(experiment, streamType).get
//            val serBuffer = ArrayBuffer[Array[Byte]]()
//            for item <- sourceDataRdf4j do
//              val oStream = new ByteArrayOutputStream()
//              serRdf4j(item, rdf4jFormat, oStream)
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
//              desJellyRdf4j(serialized, streamType)
//            }
//          else
//            val rdf4jFormat = getRdf4jFormat(experiment, streamType).get
//            times(experiment) += time {
//              for buffer <- serialized do
//                desRdf4j(
//                  new ByteArrayInputStream(buffer),
//                  rdf4jFormat,
//                )
//            }
//      } catch {
//        case e: Exception =>
//          println(f"Failed experiment with $experiment")
//          e.printStackTrace()
//      }
//
