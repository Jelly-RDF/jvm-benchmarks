package eu.ostrzyciel.jelly.benchmark.util

import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.stream.EncoderFlow
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

class RdfStreamServiceFromData(data: Either[Seq[Model], Seq[DatasetGraph]])
  extends RdfStreamService:

  import eu.ostrzyciel.jelly.convert.jena.given
  
  protected def source[T](s: Seq[T]): Source[T, NotUsed] = Source(s)

  override def subscribeRdf(in: RdfStreamSubscribe): Source[RdfStreamFrame, NotUsed] =
    val options = in.requestedOptions.get

    data match
      case Left(models) =>
        source(models)
          .map(_.asTriples)
          // Don't do this in production... simply use the options requested by the client.
          // Throw exception if the client didn't send their options.
          .via(EncoderFlow.graphStream(None, options))
      case Right(datasets) =>
        options.physicalType match
          case PhysicalStreamType.QUADS =>
            source(datasets)
              .map(_.asQuads)
              .via(EncoderFlow.datasetStreamFromQuads(None, options))
          case PhysicalStreamType.GRAPHS =>
            source(datasets)
              .map(_.asGraphs)
              .via(EncoderFlow.datasetStream(None, options))
          case _ => throw new IllegalArgumentException("Only QUADS and GRAPHS are supported")

  override def publishRdf(in: Source[RdfStreamFrame, NotUsed]): Future[RdfStreamReceived] =
    throw new UnsupportedOperationException("Not implemented")
