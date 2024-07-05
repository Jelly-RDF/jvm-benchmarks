package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.benchmark.Util
import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.jena.graph.{Graph, GraphMemFactory}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphFactory, NamedGraphWrapper}
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.util.ByteString

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.concurrent.Await
import scala.concurrent.duration.*

object DataLoader:
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  // TODO: remove this in Jena 5
  // Reimplement graph makers for the dataset factory.
  // This way we use the new (faster?) Graph implementation that will be the default in Jena 5.
  DatasetGraphFactory.graphMakerNamedGraphMem = name => {
    val g = GraphMemFactory.createDefaultGraphSameTerm()
    new NamedGraphWrapper(name, g)
  }
  DatasetGraphFactory.graphMakerMem = name => GraphMemFactory.createDefaultGraphSameTerm()

  /**
   * @param path        path to the source file
   * @param streamType  "triples" or "quads" or "graphs"
   * @param elementSize number of elements in a single chunk – set to 0 to disable chunking
   * @return (numStatements, numElements, elements)
   */
  def getSourceData(path: String, streamType: String, elementSize: Int)(implicit as: ActorSystem[_]):
  (Long, Long, Either[Seq[Model], Seq[DatasetGraph]]) =
    if path.endsWith(".jelly.gz") then
      getSourceDataJelly(path, streamType, elementSize)
    else
      getSourceDataTarGz(path, streamType, elementSize)

  /**
   * @param path path to the source file
   * @param streamType "triples" or "quads" or "graphs"
   * @param elementSize number of elements in a single chunk – set to 0 to disable chunking
   * @return (numStatements, numElements, elements)
   */
  def getSourceDataTarGz(path: String, streamType: String, elementSize: Int)(implicit as: ActorSystem[_]):
  (Long, Long, Either[Seq[Model], Seq[DatasetGraph]]) =
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
   * @param elementSize number of elements in a single chunk – set to 0 to disable chunking
   * @return (numStatements, numElements, elements)
   */
  def getSourceDataJelly(path: String, streamType: String, elementSize: Int)(implicit as: ActorSystem[_]):
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
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      (items.map(_.asQuads.size).sum, items.size, Right(items))
