package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.GrpcThroughputBench.initExperiments
import eu.ostrzyciel.jelly.benchmark.traits.Grpc
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.grpc.RdfStreamServer
import eu.ostrzyciel.jelly.stream.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

object GrpcLatencyBench extends Grpc:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  class LatencyStreamService(data: Either[Seq[Model], Seq[DatasetGraph]], interval: Option[FiniteDuration],
    elements: Int, timestamps: mutable.ArrayBuffer[Long])
  extends RdfStreamServiceFromData(data):
    override protected def source[T](s: Seq[T]): Source[T, NotUsed] =
      interval match
        case Some(i) => Source(s)
          .take(elements)
          .zip(Source.tick(0.seconds, i, 0))
          .map((t, _) => {
            timestamps += System.nanoTime()
            t
          })
        case None => Source(s)
          .take(elements)
          .map(t => {
            timestamps += System.nanoTime()
            t
          })

  /**
   * @param gzip Whether to use gzip compression
   * @param streamType The type of stream to use (triples, graphs, quads)
   * @param elements The number of elements to process or 0 to process all
   * @param sourceFilePath The path to the source file
   */
  @main
  def runGrpcLatencyBench(gzip: Boolean, streamType: String, elements: Int, sourceFilePath: String): Unit =
    initExperiments(streamType)
    loadData(sourceFilePath, streamType, elements)

    val resultMap: Map[String, mutable.Map[String, Seq[(Long, Long)]]] = experiments
      .map(_ -> mutable.Map[String, Seq[(Long, Long)]]()).toMap

    for exp <- experiments do
      println(s"Running experiment $exp")
      LatencyUtil.run(
        (i, el) => runOne(i, el, gzip, getJellyOpts(exp, streamType, true)),
        resultMap(exp)
      )
    saveRunInfo("grpc_latency", Map(
      "times" -> resultMap,
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "useGzip" -> gzip,
      "file" -> sourceFilePath,
      "streamType" -> streamType,
      "port" -> config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
    ))
    sys.exit()

  private def runOne(
    interval: Option[FiniteDuration], elements: Int, gzip: Boolean, opt: RdfStreamOptions
  ) =
    val tsServer = new mutable.ArrayBuffer[Long]()
    val tsClient = new mutable.ArrayBuffer[Long]()

    val server = {
      given ActorSystem[_] = serverSystem
      val serverOptions = RdfStreamServer.Options(
        host = "127.0.0.1",
        port = config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
        enableGzip = gzip,
      )
      val service = new LatencyStreamService(sourceData, interval, elements, tsServer)
      val s = new RdfStreamServer(serverOptions, service)
      s.run()
      s
    }

    val clientFut: Future[Done] = {
      given ActorSystem[_] = clientSystem
      val settings = GrpcClientSettings.fromConfig("jelly-rdf-client")
      val client = RdfStreamServiceClient(settings)

      val responseStream = client.subscribeRdf(RdfStreamSubscribe("dummy topic", Some(opt)))
      val s: Source[IterableOnce[Any], NotUsed] = opt.physicalType match
        case PhysicalStreamType.TRIPLES =>
          responseStream
            .via(DecoderFlow.decodeTriples.asGraphStream)
        case PhysicalStreamType.QUADS =>
          responseStream
            .via(DecoderFlow.decodeQuads.asDatasetStreamOfQuads)
        case PhysicalStreamType.GRAPHS =>
          responseStream
            .via(DecoderFlow.decodeGraphs.asDatasetStream)
        case _ => throw new Error("Unknown stream type")

      s.map(_.iterator.size)
        .runForeach(_ => tsClient.append(System.nanoTime))
    }

    Await.result(clientFut, Duration.Inf)
    Await.result(server.terminate(), Duration.Inf)

    if tsServer.size != tsClient.size then
      throw new Error(f"Message count mismatch. ${tsServer.size} server, ${tsClient.size} client")

    tsServer.zip(tsClient).toSeq
