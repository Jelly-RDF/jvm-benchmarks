//package eu.neverblink.jelly.benchmark
//
//import eu.neverblink.jelly.benchmark.traits.*
//import eu.neverblink.jelly.benchmark.util.{DataLoader, GroupedDataStream, GroupedDataStreamRdf4j}
//import eu.neverblink.jelly.convert.jena.JenaConverterFactory
//import eu.neverblink.jelly.core.ProtoEncoder
//import eu.neverblink.jelly.core.proto.v1.RdfStreamRow
//import org.apache.jena.rdf.model.Model
//import org.apache.jena.sparql.core.DatasetGraph
//import org.apache.pekko.NotUsed
//import org.apache.pekko.stream.scaladsl.*
//import org.eclipse.rdf4j.model.Statement
//
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//import scala.concurrent.duration.*
//import scala.concurrent.{Await, Future}
//
//object GroupedSizeBench extends GroupedSerDes, Size:
//  import eu.neverblink.jelly.benchmark.util.Experiments.*
//  import eu.neverblink.jelly.benchmark.util.Util.*
//  import eu.neverblink.jelly.convert.jena.given
//
//  private val sizes: mutable.Map[String, Long] = mutable.Map.empty
//
//  /**
//   * @param streamType "triples", "quads", "graphs"
//   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
//   * @param elements number of elements to process or 0 to process all
//   * @param sourceFilePath path to the source file
//   */
//  @main
//  def runGroupedSizeBench(streamType: String, elementSize: Int, elements: Int, sourceFilePath: String): Unit =
//    initExperiment(flatStreaming = false, jena = true, rdf4j = true, streamType)
//    val dataSource = DataLoader.getSourceDataAsStream(
//      sourceFilePath, streamType, elementSize,
//      if elements == 0 then None else Some(elements)
//    )
//    val rdf4jSource = DataLoader.jellySourceRdf4j(sourceFilePath, elementSize, if elements == 0 then None else Some(elements))
//    run(dataSource, rdf4jSource)
//    saveRunInfo(s"size_$streamType", Map(
//      "elements" -> numElements,
//      "statements" -> numStatements,
//      "experiments" -> experiments,
//      "sizes" -> sizes,
//      "file" -> sourceFilePath,
//      "elementSize" -> elementSize,
//      "streamType" -> streamType,
//    ))
//    sys.exit()
//
//  private def run(dataSource: GroupedDataStream, rdf4jSource: GroupedDataStreamRdf4j): Unit =
//    val resultFutures = dataSource match
//      // Graph stream
//      case Left(source) =>
//        val hub = source
//          .wireTap(m => {
//            numStatements += m.size()
//            numElements += 1
//            if numElements % 10_000 == 0 then println(s"Processed $numElements graphs")
//          })
//          .toMat(BroadcastHub.sink)(Keep.right)
//          .run()
//        getSinks[Model].map(s => hub.runWith(s))
//      // Dataset stream
//      case Right(source) =>
//        val hub = source
//          .wireTap(ds => {
//            numStatements += ds.asQuads.size
//            numElements += 1
//            if numElements % 10_000 == 0 then println(s"Processed $numElements datasets")
//          })
//          .toMat(BroadcastHub.sink)(Keep.right)
//          .run()
//        getSinks[DatasetGraph].map(s => hub.runWith(s))
//
//    val rdf4jHub = rdf4jSource
//      .wireTap(group => {
//        numStatementsRdf4j += group.size
//        numElementsRdf4j += 1
//        if numElementsRdf4j % 10_000 == 0 then println(s"Processed $numElementsRdf4j RDF4J statement groups")
//      })
//      .toMat(BroadcastHub.sink)(Keep.right)
//      .run()
//    val rdf4jResultFutures = getRdf4jSinks.map(s => rdf4jHub.runWith(s))
//
//    val results = Await.result(Future.sequence(resultFutures ++ rdf4jResultFutures), 24.hours)
//    results.foreach { case (expName, size, count) =>
//      println(f"Experiment $expName: $size bytes, $count elements")
//      sizes.updateWith(expName)(_.map(_ + size).orElse(Some(size)))
//    }
//
//  private def getSinks[T <: Model | DatasetGraph]:
//  Seq[Sink[T, Future[(String, Long, Long)]]] =
//    for
//      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
//      compressionMode <- Seq(None, Some("individual"), Some("continuous"))
//      experiment <- experiments if !experiment.startsWith("rdf4j")
//    yield
//      val expName = experiment + compressionMode.fold("")(f"-${compressionMethod}-" + _)
//      var (os, cos) = getOs(compressionMethod, compressionMode)
//      val individualCompression = compressionMode.isDefined && compressionMode.get == "individual"
//      val sink = if experiment.startsWith("jelly") then
//        val opts = getJellyOpts(experiment, streamType, grouped = false)
//        val encoder = JenaConverterFactory.encoder(ProtoEncoder.Params(
//          opts,
//          maybeRowBuffer = Some(ListBuffer[RdfStreamRow]()),
//        ))
//        Flow[T].map(m => serJellyOneElement(m, encoder, frame => {
//          frame.writeTo(os)
//          if individualCompression then
//            os.close()
//            os = reWrapOs(compressionMethod, cos)
//        }))
//      else
//        val format = getJenaFormat(experiment, streamType).get
//        Flow[T].map(m => {
//          serJena(m, format, os)
//          if individualCompression then
//            os.close()
//            os = reWrapOs(compressionMethod, cos)
//        })
//      sink
//        .recover(e => {
//          println(s"Error in experiment $experiment: $e")
//          e.printStackTrace()
//        })
//        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
//        .mapMaterializedValue(f => f.map(counter => {
//          if !individualCompression then
//            os.close()
//          (expName, cos.getCount, counter)
//        }))
//
//  private def getRdf4jSinks: Seq[Sink[Seq[Statement], Future[(String, Long, Long)]]] =
//    for
//      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
//      compressionMode <- Seq(None, Some("individual"), Some("continuous"))
//      experiment <- experiments if experiment.startsWith("rdf4j")
//    yield
//      val expName = experiment + compressionMode.fold("")(f"-${compressionMethod}-" + _)
//      var (os, cos) = getOs(compressionMethod, compressionMode)
//      val individualCompression = compressionMode.isDefined && compressionMode.get == "individual"
//
//      val format = getRdf4jFormat(experiment, streamType).get
//      val sink = Flow[Seq[Statement]].map(ds => {
//        serRdf4j(ds, format, os)
//        if individualCompression then
//          os.close()
//          os = reWrapOs(compressionMethod, cos)
//      })
//      sink
//        .recover(e => {
//          println(s"Error in experiment $experiment: $e")
//          e.printStackTrace()
//        })
//        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
//        .mapMaterializedValue(f => f.map(counter => {
//          if !individualCompression then
//            os.close()
//          (expName, cos.getCount, counter)
//        }))
