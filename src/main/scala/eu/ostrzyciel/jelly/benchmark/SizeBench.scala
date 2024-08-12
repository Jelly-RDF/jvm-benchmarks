package eu.ostrzyciel.jelly.benchmark

import com.google.common.io.CountingOutputStream
import eu.ostrzyciel.jelly.benchmark.traits.GroupedSerDes
import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, GroupedDataStream}
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import org.apache.commons.io.output.NullOutputStream
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.*

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
    initExperiment(flatStreaming = false, streamType)
    val dataSource = DataLoader.getSourceDataAsStream(
      sourceFilePath, streamType, elementSize,
      if elements == 0 then None else Some(elements)
    )
    run(dataSource)
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

  private def run(dataSource: GroupedDataStream): Unit =
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

    val results = Await.result(Future.sequence(resultFutures), 4.hours)
    results.foreach { case (expName, size, count) =>
      println(f"Experiment $expName: $size bytes, $count elements")
      sizes.updateWith(expName)(_.map(_ + size).orElse(Some(size)))
    }

  private def getSinks[T <: Model | DatasetGraph]:
  Seq[Sink[T, Future[(String, Long, Long)]]] =
    for gzip <- Seq(None, Some("individual"), Some("continuous")); experiment <- experiments yield
      val expName = experiment + gzip.fold("")("-gzip-" + _)
      var (os, cos) = getOs(gzip = gzip.isDefined)
      val individualGzip = gzip.isDefined && gzip.get == "individual"
      val sink = if experiment.startsWith("jelly") then
        val opts = getJellyOpts(experiment, streamType, grouped = false)
        val encoder = JenaConverterFactory.encoder(opts)
        Flow[T].map(m => serJellyOneElement(m, encoder, frame => {
          frame.writeTo(os)
          if individualGzip then
            os.close()
            os = new GZIPOutputStream(cos)
        }))
      else
        val format = getJenaFormat(experiment, streamType).get
        Flow[T].map(m => {
          serJena(m, format, os)
          if individualGzip then
            os.close()
            os = new GZIPOutputStream(cos)
        })
      sink
        .recover(e => {
          println(s"Error in experiment $experiment: $e")
          e.printStackTrace()
        })
        .toMat(Sink.fold(0L)((acc, _) => acc + 1L))(Keep.right)
        .mapMaterializedValue(f => f.map(counter => {
          if !individualGzip then
            os.close()
          (expName, cos.getCount, counter)
        }))

  private def getOs(gzip: Boolean): (OutputStream, CountingOutputStream) =
    val cos = new CountingOutputStream(NullOutputStream.INSTANCE)
    if gzip then
      (new java.util.zip.GZIPOutputStream(cos), cos)
    else (cos, cos)
