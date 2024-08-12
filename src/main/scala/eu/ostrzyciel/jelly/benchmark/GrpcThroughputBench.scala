package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.traits.Networked
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.grpc.RdfStreamServer
import eu.ostrzyciel.jelly.stream.DecoderFlow
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.*

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

object GrpcThroughputBench extends Networked:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  private var t0client: Map[String, ArrayBuffer[Long]] = _
  private var t0server: Map[String, ArrayBuffer[Long]] = _
  private var t1client: Map[String, ArrayBuffer[Long]] = _

  case class StreamResult(t0client: Long, t0server: Long, t1client: Long):
    def time: Long = t1client - t0server

  /**
   * @param gzip Whether to use gzip compression
   * @param streamType The type of stream to use (triples, graphs, quads)
   * @param elements The number of elements to process or 0 to process all
   * @param sourceFilePath The path to the source file
   */
  @main
  def runGrpcThroughputBench(gzip: Boolean, streamType: String, elements: Int, sourceFilePath: String): Unit =
    given ActorSystem[_] = serverSystem
    given ExecutionContext = serverSystem.executionContext

    initExperiments(streamType, useJena = false)
    loadData(sourceFilePath, streamType, elements)
    val filler = () => experiments.map(_ -> ArrayBuffer[Long]()).toMap
    t0client = filler()
    t0server = filler()
    t1client = filler()

    println("Starting server...")
    val serverOptions = RdfStreamServer.Options(
      host = "127.0.0.1",
      port = config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
      enableGzip = gzip,
    )
    val service = new RdfStreamServiceFromData(sourceData) {
      override def subscribeRdf(in: RdfStreamSubscribe): Source[RdfStreamFrame, NotUsed] =
        t0server(in.topic).append(System.nanoTime())
        super.subscribeRdf(in)
    }
    new RdfStreamServer(serverOptions, service).run() map { binding =>
      println("Started server: " + binding)
      runClient(gzip)
    } recover {
      case e: Throwable => e.printStackTrace()
    }

  private def runClient(gzip: Boolean): Unit =
    given ActorSystem[_] = clientSystem
    given ExecutionContext = clientSystem.executionContext

    val settings = GrpcClientSettings.fromConfig("jelly-rdf-client")
    val client = RdfStreamServiceClient(settings)

    for expName <- experiments; i <- 1 to ConfigManager.benchmarkNetworkRepeats do
      println(s"Experiment $expName try: $i")
      Await.result(
        request(client, getJellyOpts(expName, streamType, grouped = true), expName),
        Duration.Inf
      )

    val times = experiments.map(expName =>
      expName -> t0client(expName).lazyZip(t0server(expName)).lazyZip(t1client(expName))
        .map((t0c, t0s, t1c) => StreamResult(t0c, t0s, t1c))
        .toSeq
    ).toMap

    printSpeed(numStatements, times.map((k, v) => k -> v.map(_.time)))
    saveRunInfo("grpc_throughput", Map(
      "times" -> times,
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "useGzip" -> gzip,
      "file" -> sourceFilePath,
      "streamType" -> streamType,
      "port" -> config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
    ))
    sys.exit()

  private def request(client: RdfStreamServiceClient, opt: RdfStreamOptions, expName: String)
    (using ExecutionContext, ActorSystem[_]):
  Future[Unit] =
    System.gc()
    println("Sleeping 5 seconds...")
    val waitFuture = Future {
      Thread.sleep(5000)
    }
    waitFuture flatMap { _ =>
      t0client(expName).append(System.nanoTime())
      val responseStream = client.subscribeRdf(RdfStreamSubscribe(expName, Some(opt)))
      opt.physicalType match
        case PhysicalStreamType.TRIPLES =>
          responseStream
            .via(DecoderFlow.decodeTriples.asGraphStream)
            .runWith(countSink)
        case PhysicalStreamType.QUADS =>
          responseStream
            .via(DecoderFlow.decodeQuads.asDatasetStreamOfQuads)
            .runWith(countSink)
        case PhysicalStreamType.GRAPHS =>
          responseStream
            .via(DecoderFlow.decodeGraphs.asDatasetStream)
            .runWith(countSink)
        case _ => throw new RuntimeException("Unknown stream type")
    } map { (statements, elements) =>
      t1client(expName).append(System.nanoTime())
      val thing = if opt.physicalType.isGraphs then "graphs" else "statements"
      println(s"Streaming done, elements: $elements $thing: $statements")
    }
