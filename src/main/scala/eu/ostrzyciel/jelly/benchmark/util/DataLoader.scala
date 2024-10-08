package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.jena.graph.{Graph, GraphMemFactory, Triple}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory, Quad}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.util.ByteString

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

object DataLoader:
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  /**
   * @param path        path to the source file
   * @param streamType  "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return (numStatements, numElements, elements)
   */
  def getSourceData(path: String, streamType: String, elementSize: Int, elements: Option[Int])(using ActorSystem[_]):
  (Long, Long, GroupedData) =
    val source = if path.endsWith(".jelly.gz") then
      jellySource(path, streamType, elementSize, elements)
    else
      tarGzSource(path, streamType, elementSize, elements)
    println("Loading the source file...")
    val readFuture = source.runWith(Sink.seq)
    if streamType == "triples" then
      val items = Await.result(readFuture, 3.hours).asInstanceOf[Seq[Model]]
      (
        items.map(_.size()).sum,
        items.size,
        Left(items)
      )
    else
      val items = Await.result(readFuture, 3.hours).asInstanceOf[Seq[DatasetGraph]]
      (items.map(_.asQuads.size).sum, items.size, Right(items))

  /**
   * @param path        path to the source file
   * @param streamType  "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return Pekko Source of models or dataset graphs
   */
  def getSourceDataAsStream(path: String, streamType: String, elementSize: Int, elements: Option[Int])
                           (using ActorSystem[_]):
  GroupedDataStream =
    val source = if path.endsWith(".jelly.gz") then
      jellySource(path, streamType, elementSize, elements)
    else
      tarGzSource(path, streamType, elementSize, elements)
    if streamType == "triples" then
      Left(source.asInstanceOf[Source[Model, NotUsed]])
    else
      Right(source.asInstanceOf[Source[DatasetGraph, NotUsed]])

  /**
   * @param path        path to the source file
   * @param streamType  "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return Pekko Source of models or dataset graphs
   */
  def tarGzSource(path: String, streamType: String, elementSize: Int, elements: Option[Int])
                 (using ActorSystem[_]):
  Source[Model | DatasetGraph, NotUsed] =
    val lang = if streamType == "triples" then Lang.TTL else Lang.TRIG
    val is = TarArchiveInputStream(
      GZIPInputStream(FileInputStream(path))
    )
    val tarIterator = Iterator
      .continually(is.getNextTarEntry)
      .takeWhile(_ != null)
      .filter(is.canReadEntryData)
      .filter(_.isFile)
      .map(entry =>
        ByteString.fromArray(IOUtils.toByteArray(is))
      )

    Source.fromIterator(() => tarIterator)
      .map(bytes => {
        val parser = RDFParser.create()
          .fromString(bytes.decodeString("UTF-8"))
          .lang(lang)

        val result: Model | DatasetGraph = if streamType == "triples" then
          // Use the new Graph implementation that will be the default in Jena 5
          val m = ModelFactory.createModelForGraph(GraphMemFactory.createDefaultGraphSameTerm())
          parser.parse(m)
          m
        else
          val ds = DatasetGraphFactory.create()
          parser.parse(ds)
          ds
        result
      })
      .via(if elements.isDefined then Flow[Model | DatasetGraph].take(elements.get) else Flow[Model | DatasetGraph])

  /**
   * @param path        path to the source file
   * @param streamType  "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return Pekko Source of models or dataset graphs
   */
  def jellySource(path: String, streamType: String, elementSize: Int, elements: Option[Int])
                 (using ActorSystem[_]):
  Source[Model | DatasetGraph, NotUsed] =
    val is = GZIPInputStream(FileInputStream(path))
    if streamType == "triples" then
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.decodeTriples.asGraphStream)
      (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(ts => {
          val model = ModelFactory.createDefaultModel()
          ts.iterator.foreach(model.getGraph.add)
          model
        })
        .via(if elements.isDefined then Flow[Model].take(elements.get) else Flow[Model])
    else
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.decodeQuads.asDatasetStreamOfQuads)
      (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(qs => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          qs.iterator.foreach(dataset.add)
          dataset
        })
        .via(if elements.isDefined then Flow[DatasetGraph].take(elements.get) else Flow[DatasetGraph])


  def getSourceDataJellyFlat(path: String, streamType: String, statements: Option[Int])
                            (using ActorSystem[_], ExecutionContext): 
  Either[Seq[Triple], Seq[Quad]] =
    println("Loading the source file...")
    val is = GZIPInputStream(FileInputStream(path))
    val s = JellyIo.fromIoStream(is)
    val future = if streamType == "triples" then
      s.via(DecoderFlow.decodeTriples.asFlatTripleStream)
        .via(if statements.isDefined then Flow[Triple].take(statements.get) else Flow[Triple])
        .runWith(Sink.seq)
        .map(ts => Left(ts))
    else
      s.via(DecoderFlow.decodeQuads.asFlatQuadStream)
        .via(if statements.isDefined then Flow[Quad].take(statements.get) else Flow[Quad])
        .runWith(Sink.seq)
        .map(qs => Right(qs))

    Await.result(future, 3.hours)
