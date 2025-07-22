//package eu.neverblink.jelly.benchmark.util
//
//import eu.neverblink.jelly.core.proto.v1.*
//import eu.neverblink.jelly.pekko.stream.EncoderFlow
//import org.apache.jena.rdf.model.Model
//import org.apache.jena.sparql.core.DatasetGraph
//import org.apache.pekko.NotUsed
//import org.apache.pekko.stream.scaladsl.Source
//
//import scala.concurrent.Future
//
//class RdfStreamServiceFromData(data: GroupedData)
//  extends RdfStreamService:
//
//  import eu.neverblink.jelly.convert.jena.given
//
//  protected def source[T](s: Seq[T]): Source[T, NotUsed] = Source(s)
//
//  override def subscribeRdf(in: RdfStreamSubscribe): Source[RdfStreamFrame, NotUsed] =
//    val options = in.requestedOptions.get
//
//    data match
//      case Left(models) => source(models)
//        .map(_.asTriples)
//        // Don't do this in production... here we simply use the options requested by the client.
//        // Throw exception if the client didn't send their options.
//        .via(EncoderFlow.builder.graphs(options).flow)
//      case Right(datasets) => options.physicalType match
//        case PhysicalStreamType.QUADS =>
//          source(datasets)
//            .map(_.asQuads)
//            .via(EncoderFlow.builder.datasetsFromQuads(options).flow)
//        case PhysicalStreamType.GRAPHS =>
//          source(datasets)
//            .map(_.asGraphs)
//            .via(EncoderFlow.builder.datasets(options).flow)
//        case _ => throw new IllegalArgumentException("Only QUADS and GRAPHS are supported")
//
//  override def publishRdf(in: Source[RdfStreamFrame, NotUsed]): Future[RdfStreamReceived] =
//    throw new UnsupportedOperationException("Not implemented")
