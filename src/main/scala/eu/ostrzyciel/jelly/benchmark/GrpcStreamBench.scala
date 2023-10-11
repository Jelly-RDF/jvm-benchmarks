package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.benchmark.util.{DataLoader, RdfStreamServiceFromData}
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.grpc.RdfStreamServer
import eu.ostrzyciel.jelly.stream.DecoderFlow
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.*

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

object GrpcStreamBench:
  import Experiments.*
  import Util.*
  import eu.ostrzyciel.jelly.convert.jena.*

  val config = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
    .withFallback(ConfigFactory.load())

  implicit private val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "StreamServer", config)
  implicit private val ec: ExecutionContext = system.executionContext

  private val t0client: Map[String, ArrayBuffer[Long]] = jellyOptions.keys.map(_ -> ArrayBuffer[Long]()).toMap
  private val t0server: Map[String, ArrayBuffer[Long]] = jellyOptions.keys.map(_ -> ArrayBuffer[Long]()).toMap
  private val t1client: Map[String, ArrayBuffer[Long]] = jellyOptions.keys.map(_ -> ArrayBuffer[Long]()).toMap

  case class StreamResult(t0client: Long, t0server: Long, t1client: Long):
    def time = t1client - t0server

  // Args: [gzip? 0/1] [triples/graphs/quads] [element size] [source file path]
  def main(args: Array[String]): Unit =
    val gzip = args(0) == "1"
    val streamType = args(1)
    val elementSize = args(2).toInt
    val sourceFilePath = args(3)

    val (numStatements, numElements, data) = DataLoader.getSourceData(sourceFilePath, streamType, elementSize)

    println("Starting server...")
    val serverOptions = RdfStreamServer.Options(
      host = "127.0.0.1",
      port = config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
      enableGzip = gzip,
    )
    val service = new RdfStreamServiceFromData(data) {
      override def subscribeRdf(in: RdfStreamSubscribe) =
        t0server(in.topic).append(System.nanoTime())
        super.subscribeRdf(in)
    }
    new RdfStreamServer(serverOptions, service).run() map { binding =>
      println("Started server: " + binding)
      runClient(streamType, numElements, numStatements, sourceFilePath, gzip)
    } recover {
      case e: Throwable => e.printStackTrace()
    }

  private def runClient(streamType: String, numElements: Long, numStatements: Long, file: String, gzip: Boolean): Unit =
    implicit val clientSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamClient", config)
    implicit val ec: ExecutionContext = clientSystem.executionContext

    val settings = GrpcClientSettings.fromConfig("jelly-rdf-client")

    val client = RdfStreamServiceClient(settings)

    for i <- 1 to REPEATS; expName <- jellyOptions.keys do
      println(s"Experiment $expName try: $i")
      Await.result(
        request(client, getJellyOpts(expName, streamType), expName),
        Duration.Inf
      )

    val times = jellyOptions.keys.map(expName =>
      expName -> t0client(expName).lazyZip(t0server(expName)).lazyZip(t1client(expName))
        .map((t0c, t0s, t1c) => StreamResult(t0c, t0s, t1c))
        .toSeq
    ).toMap

    printSpeed(numStatements, times.map((k, v) => k -> v.map(_.time)))
    saveRunInfo("grpc_stream", config, Map(
      "times" -> times,
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> jellyOptions.keys.toSeq,
      "useGzip" -> gzip,
      "file" -> file,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def request(client: RdfStreamServiceClient, opt: RdfStreamOptions, expName: String)
    (implicit ec: ExecutionContext, system: ActorSystem[_]):
  Future[Unit] =
    System.gc()
    println("Sleeping 5 seconds...")
    val waitFuture = Future {
      Thread.sleep(5000)
    }
    waitFuture flatMap { _ =>
      t0client(expName).append(System.nanoTime())
      val responseStream = client.subscribeRdf(RdfStreamSubscribe(expName, Some(opt)))
      opt.streamType match
        case RdfStreamType.TRIPLES =>
          responseStream
            .via(DecoderFlow.triplesToGrouped)
            .runWith(countSink)
        case RdfStreamType.QUADS =>
          responseStream
            .via(DecoderFlow.quadsToGrouped)
            .runWith(countSink)
        case RdfStreamType.GRAPHS =>
          responseStream
            .via(DecoderFlow.graphsToFlat)
            .map(_._2)
            .runWith(countSink)
        case _ => throw new RuntimeException("Unknown stream type")
    } map { (statements, elements) =>
      t1client(expName).append(System.nanoTime())
      println(s"Streaming done, elements: $elements statements: $statements")
    }

  private def countSink[T]: Sink[IterableOnce[T], Future[(Long, Long)]] =
    Sink.fold((0L, 0L))((counter, els) =>
      (counter._1 + els.iterator.size, counter._2 + 1)
    )
