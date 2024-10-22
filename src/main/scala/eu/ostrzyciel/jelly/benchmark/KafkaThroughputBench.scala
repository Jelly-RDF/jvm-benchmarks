package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.traits.Kafka
import eu.ostrzyciel.jelly.benchmark.util.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.*
import org.apache.pekko.kafka.scaladsl.{Consumer, Producer}
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.{Done, NotUsed}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

object KafkaThroughputBench extends Kafka:
  import eu.ostrzyciel.jelly.benchmark.util.Util.*

  case class KafkaResult(t0cons: Long, t0prod: Long, t1cons: Long, t1prod: Long, bytes: Long):
    def time: Long = t1cons - t0prod

  private var times: Map[String, ArrayBuffer[KafkaResult]] = _

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
      if !isForbiddenCombination(expName, sourceFilePath) then
        println(s"Experiment $expName try: $i")
        times(expName) += Await.result(
          runTest(expName) recover {
            case e =>
              println(s"Error in experiment $expName: $e")
              e.printStackTrace() ; KafkaResult(-1, -1, -1, -1, -1)
          },
          Duration.Inf
        )
        System.gc()
        recreateTopic()
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

    val deserializer = getDeserializer(experiment)
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

  private def runConsumer(deserializer: Flow[Array[Byte], Unit, NotUsed],
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
      .take(numElements)
      .completionTimeout(20.minutes)
      .run()
    consumerFuture map {
      _ => totalSize
    } recover {
      case e =>
        println(s"Error in consumer: $e")
        e.printStackTrace()
        -1
    }

  private def runProducer(serializer: Flow[ModelOrDataset, Array[Byte], NotUsed],
                          settings: ProducerSettings[String, Array[Byte]])(using system: ActorSystem[Nothing]):
  Future[Done] =
    given ExecutionContext = system.executionContext
    val kafkaProd = Producer.plainSink(settings)
    Source[ModelOrDataset](sourceData.fold(identity, identity))
      .via(serializer)
      .map(bytes => new ProducerRecord[String, Array[Byte]]("rdf", bytes))
      // .recover()
      .runWith(kafkaProd)
