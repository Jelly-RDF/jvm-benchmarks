package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.benchmark.Util
import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.concurrent.Await
import scala.concurrent.duration.*

object DataLoader:
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  /**
   * @param path path to the source file
   * @param streamType "triples" or "quads" or "graphs"
   * @param elementSize number of elements in a single chunk – set to 0 to disable chunking
   * @return (numStatements, numElements, elements)
   */
  def getSourceData(path: String, streamType: String, elementSize: Int)(implicit as: ActorSystem[_]):
  (Long, Long, Either[Seq[Model], Seq[DatasetGraph]]) =
    println("Loading the source file...")
    val is = GZIPInputStream(FileInputStream(path))
    if streamType == "triples" then
      val s = JellyIo.fromIoStream(is)
        .via(DecoderFlow.decodeTriples.asGraphStream())

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
        .via(DecoderFlow.decodeQuads.asDatasetStreamOfQuads())

      val readFuture = (if elementSize == 0 then s else s.mapConcat(identity).grouped(elementSize))
        .map(qs => {
          val dataset = DatasetFactory.create().asDatasetGraph()
          qs.iterator.foreach(dataset.add)
          dataset
        })
        .runWith(Sink.seq)

      val items = Await.result(readFuture, 3.hours)
      (items.map(_.asQuads.size).sum, items.size, Right(items))
