package eu.ostrzyciel.jelly.benchmark

import com.google.common.io.CountingOutputStream
import eu.ostrzyciel.jelly.benchmark.traits.*
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.convert.jena.riot.*
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.commons.io.output.NullOutputStream
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sys.JenaSystem
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.*
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

object FlatSizeBench extends FlatSerDes, Size:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*

  private val sizes: mutable.Map[String, Long] = mutable.Map.empty

  /**
   * @param streamType "triples" or "quads"
   * @param statements number of statements to process or 0 to process all
   * @param sourceFilePath path to the source file
   */
  @main
  def runFlatSizeBench(streamType: String, statements: Int, sourceFilePath: String): Unit =
    JenaSystem.init()
    initExperiment(flatStreaming = true, jena = true, rdf4j = true, streamType)
    val stOption = if statements == 0 then None else Some(statements)
    val dataSource = DataLoader.jellySourceFlat(sourceFilePath, streamType, stOption)
    val rdf4jSource = DataLoader.jellySourceRdf4jFlat(sourceFilePath, stOption)
    run(dataSource, rdf4jSource)
    saveRunInfo(s"size_flat_$streamType", Map(
      "statementLimit" -> statements,
      "statements" -> numStatements,
      "statementsRdf4j" -> numStatementsRdf4j,
      "experiments" -> experiments,
      "sizes" -> sizes,
      "file" -> sourceFilePath,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def run(dataSource: FlatDataStream, rdf4jSource: FlatDataStreamRdf4j): Unit =
    val resultFutures = dataSource match
      // Triple stream
      case Left(source) =>
        val hub = source
          .wireTap(t => {
            numStatements += 1
            if numStatements % 100_000 == 0 then println(s"Processed $numStatements statements")
          })
          .toMat(BroadcastHub.sink)(Keep.right)
          .run()
        getSinks[Triple].map(s => hub.runWith(s))
      // Dataset stream
      case Right(source) =>
        val hub = source
          .wireTap(q => {
            numStatements += 1
            if numStatements % 100_000 == 0 then println(s"Processed $numStatements statements")
          })
          .toMat(BroadcastHub.sink)(Keep.right)
          .run()
        getSinks[Quad].map(s => hub.runWith(s))

    val rdf4jHub = rdf4jSource
      .wireTap(group => {
        numStatementsRdf4j += 1
        if numStatementsRdf4j % 100_000 == 0 then println(s"Processed $numStatementsRdf4j RDF4J statements")
      })
      .toMat(BroadcastHub.sink)(Keep.right)
      .run()
    val rdf4jResultFutures = getRdf4jSinks.map(s => rdf4jHub.runWith(s))

    val results = Await.result(Future.sequence(resultFutures ++ rdf4jResultFutures), 24.hours)
    results.foreach { case (expName, size, count) =>
      println(f"Experiment $expName: $size bytes, $count elements")
      sizes.updateWith(expName)(_.map(_ + size).orElse(Some(size)))
    }

  private def getSinks[T <: TripleOrQuad]:
  Seq[Sink[T, Future[(String, Long, Long)]]] =
    for
      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
      compressionMode <- Seq(None, Some("continuous"))
      experiment <- experiments if !experiment.startsWith("rdf4j")
    yield
      val expName = experiment + compressionMode.fold("")(f"-$compressionMethod-" + _)
      val (os, cos) = getOs(compressionMethod, compressionMode)
      val context = RIOT.getContext.copy()
      val format = if experiment.startsWith("jelly") then
        val opts = getJellyOpts(experiment, streamType, grouped = false)
        context.set(JellyLanguage.SYMBOL_STREAM_OPTIONS, opts)
          .set(JellyLanguage.SYMBOL_FRAME_SIZE, 512)
        JellyFormat.JELLY_SMALL_STRICT // this gets overridden by the context
      else
        getJenaFormat(experiment, streamType).get
      val writer = StreamRDFWriter.getWriterStream(os, format, context)
      writer.start()
      Flow[T]
        .map {
          case triple: Triple => writer.triple(triple)
          case quad: Quad => writer.quad(quad)
        }
        .recover(e => {
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
        })
        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
        .mapMaterializedValue(f => f.map(counter => {
          writer.finish()
          os.close()
          (expName, cos.getCount, counter)
        }))

  private def getRdf4jSinks: Seq[Sink[Statement, Future[(String, Long, Long)]]] =
    for
      compressionMethod <- Seq("gzip", "zstd3", "zstd9")
      compressionMode <- Seq(None, Some("continuous"))
      experiment <- experiments if experiment.startsWith("rdf4j")
    yield
      val expName = experiment + compressionMode.fold("")(f"-$compressionMethod-" + _)
      val (os, cos) = getOs(compressionMethod, compressionMode)
      val format = getRdf4jFormat(experiment, streamType).get
      val writer = rio.Rio.createWriter(format, os)
      writer.startRDF()
      Flow[Statement]
        .map(writer.handleStatement)
        .recover(e => {
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
        })
        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
        .mapMaterializedValue(f => f.map(counter => {
          writer.endRDF()
          os.close()
          (expName, cos.getCount, counter)
        }))
