package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamOptions, RdfStreamType}
import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFFormat
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.stream.scaladsl.Sink

import java.io.{ByteArrayOutputStream, FileInputStream, OutputStream}
import java.util.zip.GZIPInputStream
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

object SizeBench extends SerDesBench:
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.*

  private val sizes: mutable.Map[String, Long] = mutable.Map.empty

  // Arguments: [triples/graphs/quads] [element size] [source file path]
  def main(args: Array[String]): Unit =
    val streamType = args(0)
    val elementSize = args(1).toInt
    val filePath = args(2)
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val experiments2 = (if streamType != "graphs" then experiments else Random.shuffle(jellyOptions.keys)).toSeq

    val data = getSourceData(filePath, streamType, elementSize)
    run(data, experiments2, streamType)

    saveRunInfo(s"size_${args(0)}", conf, Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "sizes" -> sizes,
      "file" -> filePath,
      "elementSize" -> elementSize,
      "streamType" -> streamType,
    ))
    sys.exit()

  def run(data: Either[Seq[Model], Seq[DatasetGraph]], exps: Seq[String], streamType: String): Unit =
    for gzip <- Seq(false, true); experiment <- exps do
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

      if experiment.startsWith("jelly") then
        serJelly(data, getJellyOpts(experiment, streamType), frame => {
          val (os, baos) = getOs
          frame.writeTo(os)
          os.close()
          sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))
        })
      else
        val sourceFlat = data match
          case Left(v) => v
          case Right(v) => v
        for item <- sourceFlat do
          val (os, baos) = getOs
          serJena(item, getFormat(experiment, streamType), os)
          os.close()
          sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))

  private def getFormat(exp: String, streamType: String): RDFFormat =
    val tuple = jenaFormats(exp)
    if streamType == "triples" then tuple(0) else tuple(1)

  private def getJellyOpts(exp: String, streamType: String): RdfStreamOptions =
    jellyOptions(exp).withStreamType(
      streamType match
        case "triples" => RdfStreamType.TRIPLES
        case "graphs" => RdfStreamType.GRAPHS
        case "quads" => RdfStreamType.QUADS
    )

  def getSourceData(path: String, streamType: String, elementSize: Int): Either[Seq[Model], Seq[DatasetGraph]] =
    println("Loading the source file...")
    val is = GZIPInputStream(FileInputStream(path))
    if streamType == "triples" then
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.triplesToGrouped)

      val readFuture = (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(ts => {
          val model = ModelFactory.createDefaultModel()
          ts.iterator.foreach(model.getGraph.add)
          model
        })
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      numStatements = items.map(_.size()).sum
      numElements = items.size
      Left(items)
    else
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.quadsToGrouped)

      val readFuture = (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(qs => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          qs.iterator.foreach(dataset.add)
          dataset
        })
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      numStatements = items.map(_.asQuads.size).sum
      numElements = items.size
      Right(items)
