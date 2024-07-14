package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.jena.graph.{Graph, GraphMemFactory, Triple}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory, Quad}
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
    if path.endsWith(".jelly.gz") then
      getSourceDataJelly(path, streamType, elementSize, elements)
    else
      getSourceDataTarGz(path, streamType, elementSize, elements)

  /**
   * @param path path to the source file
   * @param streamType "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return (numStatements, numElements, elements)
   */
  def getSourceDataTarGz(path: String, streamType: String, elementSize: Int, elements: Option[Int])
                        (using ActorSystem[_]):
  (Long, Long, GroupedData) =
    println("Loading the source file...")
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

    val readFuture = Source.fromIterator(() => tarIterator)
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
      .runWith(Sink.seq)

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
   * @param path path to the source file
   * @param streamType "triples" or "quads" or "graphs"
   * @param elementSize number of statements in a single chunk – set to 0 to disable chunking
   * @param elements    number of elements to process or None to process all
   * @return (numStatements, numElements, elements)
   */
  def getSourceDataJelly(path: String, streamType: String, elementSize: Int, elements: Option[Int])
                        (using ActorSystem[_]):
  (Long, Long, Either[Seq[Model], Seq[DatasetGraph]]) =
    println("Loading the source file...")
    val is = GZIPInputStream(FileInputStream(path))
    if streamType == "triples" then
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.decodeTriples.asGraphStream)

      val readFuture = (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(ts => {
          val model = ModelFactory.createDefaultModel()
          ts.iterator.foreach(model.getGraph.add)
          model
        })
        .via(if elements.isDefined then Flow[Model].take(elements.get) else Flow[Model])
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      (items.map(_.size()).sum, items.size, Left(items))
    else
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.decodeQuads.asDatasetStreamOfQuads)

      val readFuture = (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(qs => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          qs.iterator.foreach(dataset.add)
          dataset
        })
        .via(if elements.isDefined then Flow[DatasetGraph].take(elements.get) else Flow[DatasetGraph])
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      (items.map(_.asQuads.size).sum, items.size, Right(items))

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
