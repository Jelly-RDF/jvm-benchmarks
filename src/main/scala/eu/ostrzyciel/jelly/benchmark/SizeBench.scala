package eu.ostrzyciel.jelly.benchmark

import com.google.common.io.CountingOutputStream
import eu.ostrzyciel.jelly.benchmark.traits.GroupedSerDes
import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, GroupedDataStream, GroupedDataStreamRdf4j}
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.commons.io.output.NullOutputStream
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.*
import org.eclipse.rdf4j.model.Statement

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

object SizeBench extends GroupedSerDes:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  private val sizes: mutable.Map[String, Long] = mutable.Map.empty

  /**
   * @param streamType "triples", "quads", "graphs"
   * @param elementSize 0 for the same as in the original stream, or a number for the size of the elements
   * @param elements number of elements to process or 0 to process all
   * @param sourceFilePath path to the source file
   */
  @main
  def runSizeBench(streamType: String, elementSize: Int, elements: Int, sourceFilePath: String): Unit =
    initExperiment(flatStreaming = false, rdf4j = true, streamType)
    val dataSource = DataLoader.getSourceDataAsStream(
      sourceFilePath, streamType, elementSize,
      if elements == 0 then None else Some(elements)
    )
    val rdf4jSource = DataLoader.jellySourceRdf4j(sourceFilePath, elementSize, if elements == 0 then None else Some(elements))
    run(dataSource, rdf4jSource)
    saveRunInfo(s"size_$streamType", Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "experiments" -> experiments,
      "sizes" -> sizes,
      "file" -> sourceFilePath,
      "elementSize" -> elementSize,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def run(dataSource: GroupedDataStream, rdf4jSource: GroupedDataStreamRdf4j): Unit =
    val resultFutures = dataSource match
      // Graph stream
      case Left(source) =>
        val hub = source
          .wireTap(m => {
            numStatements += m.size()
            numElements += 1
            if numElements % 10_000 == 0 then println(s"Processed $numElements graphs")
          })
          .toMat(BroadcastHub.sink)(Keep.right)
          .run()
        getSinks[Model].map(s => hub.runWith(s))
      // Dataset stream
      case Right(source) =>
        val hub = source
          .wireTap(ds => {
            numStatements += ds.asQuads.size
            numElements += 1
            if numElements % 10_000 == 0 then println(s"Processed $numElements datasets")
          })
          .toMat(BroadcastHub.sink)(Keep.right)
          .run()
        getSinks[DatasetGraph].map(s => hub.runWith(s))

    val rdf4jHub = rdf4jSource
      .wireTap(_ => {
        numStatements += 1
        numElements += 1
        if numElements % 10_000 == 0 then println(s"Processed $numElements RDF4J statements")
      })
      .toMat(BroadcastHub.sink)(Keep.right)
      .run()
    val rdf4jResultFutures = getRdf4jSinks.map(s => rdf4jHub.runWith(s))

    val results = Await.result(Future.sequence(resultFutures ++ rdf4jResultFutures), 4.hours)
    results.foreach { case (expName, size, count) =>
      println(f"Experiment $expName: $size bytes, $count elements")
      sizes.updateWith(expName)(_.map(_ + size).orElse(Some(size)))
    }

  private def getSinks[T <: Model | DatasetGraph]:
  Seq[Sink[T, Future[(String, Long, Long)]]] =
    for
      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
      compressionMode <- Seq(None, Some("individual"), Some("continuous"))
      experiment <- experiments
    yield
      val expName = experiment + compressionMode.fold("")(f"-${compressionMethod}-" + _)
      var (os, cos) = getOs(compressionMethod, compressionMode)
      val individualCompression = compressionMode.isDefined && compressionMode.get == "individual"
      val sink = if experiment.startsWith("jelly") then
        val opts = getJellyOpts(experiment, streamType, grouped = false)
        val encoder = JenaConverterFactory.encoder(opts)
        Flow[T].map(m => serJellyOneElement(m, encoder, frame => {
          frame.writeTo(os)
          if individualCompression then
            os.close()
            os = reWrapOs(compressionMethod, cos)
        }))
      else
        val format = getJenaFormat(experiment, streamType).get
        Flow[T].map(m => {
          serJena(m, format, os)
          if individualCompression then
            os.close()
            os = reWrapOs(compressionMethod, cos)
        })
      sink
        .recover(e => {
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
        })
        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
        .mapMaterializedValue(f => f.map(counter => {
          if !individualCompression then
            os.close()
          (expName, cos.getCount, counter)
        }))

  private def getRdf4jSinks: Seq[Sink[Seq[Statement], Future[(String, Long, Long)]]] =
    for
      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
      compressionMode <- Seq(None, Some("individual"), Some("continuous"))
      experiment <- experiments
    yield
      val expName = experiment + compressionMode.fold("")(f"-${compressionMethod}-" + _)
      var (os, cos) = getOs(compressionMethod, compressionMode)
      val individualCompression = compressionMode.isDefined && compressionMode.get == "individual"

      val format = getRdf4jFormat(experiment, streamType).get
      val sink = Flow[Seq[Statement]].map(ds => {
        serRdf4j(ds, format, os)
        if individualCompression then
          os.close()
          os = reWrapOs(compressionMethod, cos)
      })
      sink
        .recover(e => {
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
        })
        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
        .mapMaterializedValue(f => f.map(counter => {
          if !individualCompression then
            os.close()
          (expName, cos.getCount, counter)
        }))

  private def getOs(method: String, mode: Option[String]): (OutputStream, CountingOutputStream) =
    val cos = new CountingOutputStream(NullOutputStream.INSTANCE)
    if mode.isDefined then
      (reWrapOs(method, cos), cos)
    else (cos, cos)

  private def reWrapOs(method: String, cos: OutputStream): OutputStream =
    if method == "gzip" then new GZIPOutputStream(cos)
    else if method == "zstd3" then new ZstdCompressorOutputStream(cos, 3)
    else if method == "zstd9" then new ZstdCompressorOutputStream(cos, 9)
    else cos
