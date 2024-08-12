package eu.ostrzyciel.jelly.benchmark

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.benchmark.traits.Networked
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.stream.{DecoderFlow, EncoderFlow, JellyIo}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFWriter
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.*
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.*
import org.apache.pekko.kafka.scaladsl.{Consumer, Producer}
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.{Done, NotUsed}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

object KafkaThroughputBench extends Networked:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.benchmark.util.Util.*
  import eu.ostrzyciel.jelly.convert.jena.given

  case class KafkaResult(t0cons: Long, t0prod: Long, t1cons: Long, t1prod: Long, bytes: Long):
    def time: Long = t1cons - t0prod

  private var times: Map[String, ArrayBuffer[KafkaResult]] = _
  private var useGzip: Boolean = _

  private val prodSettings = ProducerSettings(serverSystem, new StringSerializer, new ByteArraySerializer)
    .withProperty(ProducerConfig.RETRIES_CONFIG, "1000000")
    .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")
  private val consSettings = ConsumerSettings(clientSystem, new StringDeserializer, new ByteArrayDeserializer)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  /**
   * @param gzip Whether to use gzip compression
   * @param streamType The type of stream to use (triples, graphs, quads)
   * @param elements The number of elements to process or 0 to process all
   * @param sourceFilePath The path to the source file
   */
  @main
  def runKafkaThroughputBench(gzip: Boolean, streamType: String, elements: Int, sourceFilePath: String): Unit =
    given ActorSystem[_] = serverSystem
    given ExecutionContext = serverSystem.executionContext

    initExperiments(streamType, useJena = true)
    loadData(sourceFilePath, streamType, elements)
    times = experiments.map(_ -> ArrayBuffer[KafkaResult]()).toMap
    useGzip = gzip

    for expName <- experiments; i <- 1 to ConfigManager.benchmarkNetworkRepeats do
      println(s"Experiment $expName try: $i")
      times(expName) += Await.result(
        runTest(expName),
        Duration.Inf
      )
      println("Sleeping 5 seconds...")
      Thread.sleep(5000)

    saveRunInfo("kafka_throughput", Map(
      "times" -> times,
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "useGzip" -> useGzip,
      "file" -> sourceFilePath,
      "serverProducer" -> config.getString("pekko.kafka.producer.kafka-clients.bootstrap.servers"),
      "serverConsumer" -> config.getString("pekko.kafka.consumer.kafka-clients.bootstrap.servers"),
      "streamType" -> streamType,
    ))
    sys.exit()

  private def runTest(experiment: String): Future[KafkaResult] =
    given ExecutionContext = clientSystem.executionContext
    val t0cons: Long = System.nanoTime()
    var t0prod: Long = -1
    var t1cons: Long = -1
    var t1prod: Long = -1
    var bytesRec: Long = -1

    def inputStream(bytes: Array[Byte]): InputStream =
      var is: InputStream = new ByteArrayInputStream(bytes)
      if useGzip then
        is = new GZIPInputStream(is)
      is

    val deserializer: Flow[Array[Byte], IterableOnce[TripleOrQuad], NotUsed] = if experiment.startsWith("jelly") then
      val decoder: Flow[RdfStreamFrame, IterableOnce[TripleOrQuad], NotUsed] =
        if streamType == "triples" then DecoderFlow.decodeTriples.asGraphStream
        else if streamType == "quads" then DecoderFlow.decodeQuads.asDatasetStreamOfQuads
        else DecoderFlow.decodeGraphs.asDatasetStreamOfQuads
      JellyIo.fromBytes.via(decoder)
    else
      val jenaLang = getJenaFormat(experiment, streamType).get.getLang
      Flow[Array[Byte]]
        .map(bytes => AsyncParser.asyncParseTriples(inputStream(bytes), jenaLang, "").asScala)

    val consFut = runConsumer(deserializer, consSettings)(using clientSystem)
    val consFut1 = consFut map { bytes =>
      t1cons = System.nanoTime()
      bytesRec = bytes
    }

    println("Sleeping 2 seconds...")
    val waitFuture = Future {
      Thread.sleep(2000)
    }

    val serializer = getSerializer(experiment, config)
    val prodFut = waitFuture flatMap { _ =>
      t0prod = System.nanoTime()
      runProducer(serializer, prodSettings)(using serverSystem)
    } map { _ =>
      t1prod = System.nanoTime()
    }

    for
      _ <- consFut1
      _ <- prodFut
    yield
      KafkaResult(t0cons, t0prod, t1cons, t1prod, bytesRec)

  private def getSerializer(experiment: String, config: Config): Flow[ModelOrDataset, Array[Byte], NotUsed] =
    def outputStream(): (ByteArrayOutputStream, OutputStream) =
      val bos = new ByteArrayOutputStream()
      if useGzip then
        (bos, GZIPOutputStream(bos))
      else
        (bos, bos)

    if experiment.startsWith("jelly") then
      val opt = getJellyOpts(experiment, streamType, grouped = true)
      val encoderFlow: Flow[ModelOrDataset, RdfStreamFrame, NotUsed] = if streamType == "triples" then
        Flow[ModelOrDataset].map(_.asInstanceOf[Model].asTriples).via(EncoderFlow.graphStream(None, opt))
      else if streamType == "quads" then
        Flow[ModelOrDataset].map(_.asInstanceOf[DatasetGraph].asQuads).via(EncoderFlow.datasetStreamFromQuads(None, opt))
      else
        Flow[ModelOrDataset].map(_.asInstanceOf[DatasetGraph].asGraphs).via(EncoderFlow.datasetStream(None, opt))
      encoderFlow.map(frame => {
        val (bos, os) = outputStream()
        frame.writeTo(os)
        os.close()
        bos.toByteArray
      })
    else
      val jenaFormat = getJenaFormat(experiment, streamType).get
      if streamType == "triples" then
        Flow[ModelOrDataset].map(m => {
          val (bos, os) = outputStream()
          RDFWriter.create()
            .format(jenaFormat)
            .source(m.asInstanceOf[Model])
            .output(os)
          os.close()
          bos.toByteArray
        })
      else
        Flow[ModelOrDataset].map(ds => {
          val (bos, os) = outputStream()
          RDFWriter.create()
            .format(jenaFormat)
            .source(ds.asInstanceOf[DatasetGraph])
            .output(os)
          os.close()
          bos.toByteArray
        })

  private def runConsumer(deserializer: Flow[Array[Byte], IterableOnce[TripleOrQuad], NotUsed],
                          settings: ConsumerSettings[String, Array[Byte]])(using system: ActorSystem[Nothing]):
  Future[Long] =
    given ExecutionContext = system.executionContext
    val kafkaCons = Consumer.plainSource(
      settings,
      Subscriptions.assignmentOffsetsForTimes(
        new TopicPartition("rdf", 0) -> (System.currentTimeMillis - 100)
      )
    )
    var totalSize: Long = 0
    val consumerFuture = kafkaCons
      .map(cr => {
        totalSize += cr.serializedValueSize()
        cr.value()
      })
      .via(deserializer)
      .map(elements => {
        // Enumerate the statements to make sure they are all deserialized
        elements.iterator.foreach(_ => ())
      })
      .take(numElements)
      .run()
    consumerFuture map { _ => totalSize }

  private def runProducer(serializer: Flow[ModelOrDataset, Array[Byte], NotUsed],
                          settings: ProducerSettings[String, Array[Byte]])(using system: ActorSystem[Nothing]):
  Future[Done] =
    given ExecutionContext = system.executionContext
    val kafkaProd = Producer.plainSink(settings)
    Source[ModelOrDataset](sourceData.fold(identity, identity))
      .via(serializer)
      .map(bytes => new ProducerRecord[String, Array[Byte]]("rdf", bytes))
      .runWith(kafkaProd)
