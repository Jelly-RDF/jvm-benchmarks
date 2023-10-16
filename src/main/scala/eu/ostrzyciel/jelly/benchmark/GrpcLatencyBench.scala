package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, LatencyUtil, RdfStreamServiceFromData}
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.grpc.RdfStreamServer
import eu.ostrzyciel.jelly.stream.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.Source

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.*

object GrpcLatencyBench:
  import Experiments.*
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.*

  val config = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
    .withFallback(ConfigFactory.load())

  val serverSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamServer", config)
  val clientSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamClient", config)

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

  // Arguments: [gzip? 0/1] [triples/graphs/quads] [source file path]
  def main(args: Array[String]): Unit =
    println("Loading data...")
    val gzip = args(0) == "1"
    val streamType = args(1)
    val sourceFilePath = args(2)

    val (numStatements, numElements, data) = DataLoader
      .getSourceData(sourceFilePath, streamType, 0)(serverSystem)

    val resultMap: Map[String, mutable.Map[String, Seq[(Long, Long)]]] = jellyOptionsSmall.keys
      .map(_ -> mutable.Map[String, Seq[(Long, Long)]]()).toMap

    for exp <- jellyOptionsSmall.keys do
      println(s"Running experiment $exp")
      LatencyUtil.run(
        (i, el) => runOne(data, i, el, gzip, getJellyOpts(exp, streamType)),
        resultMap(exp)
      )
    saveRunInfo("grpc_latency", config, Map(
      "times" -> resultMap,
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> jellyOptionsSmall.keys.toSeq,
      "useGzip" -> gzip,
      "file" -> sourceFilePath,
      "streamType" -> streamType,
      "port" -> config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
    ))
    sys.exit()

  def runOne(sourceData: Either[Seq[Model], Seq[DatasetGraph]], interval: Option[FiniteDuration], elements: Int,
    gzip: Boolean, opt: RdfStreamOptions) =
    val tsServer = new mutable.ArrayBuffer[Long]()
    val tsClient = new mutable.ArrayBuffer[Long]()

    val server = {
      implicit val sys: ActorSystem[_] = serverSystem
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

    val clientFut = {
      implicit val sys: ActorSystem[_] = clientSystem
      val settings = GrpcClientSettings.fromConfig("jelly-rdf-client")
      val client = RdfStreamServiceClient(settings)

      val responseStream = client.subscribeRdf(RdfStreamSubscribe("dummy topic", Some(opt)))
      val s: Source[IterableOnce[Any], NotUsed] = opt.streamType match
        case RdfStreamType.TRIPLES =>
          responseStream
            .via(DecoderFlow.triplesToGrouped)
        case RdfStreamType.QUADS =>
          responseStream
            .via(DecoderFlow.quadsToGrouped)
        case RdfStreamType.GRAPHS =>
          responseStream
            .via(DecoderFlow.graphsToFlat)
            .map(_._2)
        case _ => throw new Error("Unknown stream type")

        s.map(_.iterator.size)
          .runForeach(_ =>
            tsClient.append(System.nanoTime)
          )
    }

    Await.result(clientFut, Duration.Inf)
    Await.result(server.terminate(), Duration.Inf)

    if tsServer.size != tsClient.size then
      throw new Error("Message count mismatch")

    tsServer.zip(tsClient).toSeq
