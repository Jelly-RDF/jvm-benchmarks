package eu.neverblink.jelly.benchmark

import eu.neverblink.jelly.benchmark.GrpcThroughputBench.initExperiments
import eu.neverblink.jelly.benchmark.traits.Networked
import eu.neverblink.jelly.benchmark.util.ConfigManager.LatencyCase
import eu.neverblink.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.grpc.RdfStreamServer
import eu.ostrzyciel.jelly.stream.*
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

object GrpcLatencyBench extends Networked:
  import eu.neverblink.jelly.benchmark.util.Experiments.*
  import eu.neverblink.jelly.benchmark.util.Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  /**
   * @param gzip Whether to use gzip compression
   * @param streamType The type of stream to use (triples, graphs, quads)
   * @param sourceFilePath The path to the source file
   */
  @main
  def runGrpcLatencyBench(gzip: Boolean, streamType: String, sourceFilePath: String): Unit =
    initExperiments(streamType, useJena = false)
    loadData(sourceFilePath, streamType, ConfigManager.benchmarkLatencyCases.map(_._1).max)

    val resultMap: Map[String, mutable.Map[String, Seq[(Long, Long)]]] = experiments
      .map(_ -> mutable.Map[String, Seq[(Long, Long)]]()).toMap

    for exp <- experiments do
      println("\n" + "=" * 80 + "\n")
      println(s"Running experiment $exp")
      LatencyUtil.run(
        c => runOne(c, gzip, getJellyOpts(exp, streamType, true)),
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

  private def runOne(latencyCase: LatencyCase, gzip: Boolean, opt: RdfStreamOptions) =
    val tsServer = new mutable.ArrayBuffer[Long]()
    val tsClient = new mutable.ArrayBuffer[Long]()

    val server = {
      given ActorSystem[?] = serverSystem
      val serverOptions = RdfStreamServer.Options(
        host = "127.0.0.1",
        port = config.getInt("pekko.grpc.client.jelly-rdf-client.port"),
        enableGzip = gzip,
      )
      val service = new LatencyUtil.LatencyStreamService(sourceData, latencyCase, tsServer)
      val s = new RdfStreamServer(serverOptions, service)
      s.run()
      s
    }

    val clientFut: Future[Done] = {
      given ActorSystem[?] = clientSystem
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
