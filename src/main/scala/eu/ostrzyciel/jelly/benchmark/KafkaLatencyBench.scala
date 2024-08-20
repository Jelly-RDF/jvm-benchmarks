package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.traits.Kafka
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.benchmark.util.ConfigManager.LatencyCase
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.*
import org.apache.pekko.kafka.scaladsl.{Consumer, Producer}
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.{Done, NotUsed}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

object KafkaLatencyBench extends Kafka:
  import eu.ostrzyciel.jelly.benchmark.util.Util.*

  /**
   * @param gzip Whether to use gzip compression
   * @param streamType The type of stream to use (triples, graphs, quads)
   * @param sourceFilePath The path to the source file
   */
  @main
  def runKafkaLatencyBench(gzip: Boolean, streamType: String, sourceFilePath: String): Unit =
    given ActorSystem[_] = serverSystem
    given ExecutionContext = serverSystem.executionContext

    initExperiments(streamType, useJena = true)
    loadData(sourceFilePath, streamType, ConfigManager.benchmarkLatencyCases.map(_._1).max)
    val resultMap: Map[String, mutable.Map[String, Seq[(Long, Long)]]] = experiments
      .map(_ -> mutable.Map[String, Seq[(Long, Long)]]()).toMap
    useGzip = gzip

    for expName <- experiments do
      if !isForbiddenCombination(expName, sourceFilePath) then
        println("\n" + "=" * 80 + "\n")
        println(s"Running experiment $expName")
  
        LatencyUtil.run(
          c => {
            val runFuture = runOne(c, expName)
              .recover { case e =>
                println(s"Error in experiment $expName: $e")
                e.printStackTrace()
                Seq()
              }
            Await.result(runFuture, Duration.Inf)
          },
          resultMap(expName)
        )

    saveRunInfo("kafka_latency", Map(
      "times" -> resultMap,
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

  private def runOne(latencyCase: LatencyCase, experiment: String): Future[Seq[(Long, Long)]] =
    given ExecutionContext = clientSystem.executionContext
    val tsProducer = new mutable.ArrayBuffer[Long]()
    val tsConsumer = new mutable.ArrayBuffer[Long]()

    recreateTopic()
    val deserializer = getDeserializer(experiment)
    val consumerFuture = runConsumer(deserializer, consSettings, tsConsumer, latencyCase)(using clientSystem)

    println("Sleeping 2 seconds...")
    val waitFuture = Future {
      Thread.sleep(2000)
    }

    val serializer = getSerializer(experiment, config)
    val producerFuture = waitFuture flatMap { _ =>
      runProducer(serializer, prodSettings, tsProducer, latencyCase)(using serverSystem)
    }

    for
      _ <- consumerFuture
      _ <- producerFuture
    yield
      tsProducer.lazyZip(tsConsumer).toSeq

  private def runConsumer(deserializer: Flow[Array[Byte], IterableOnce[TripleOrQuad], NotUsed],
                          settings: ConsumerSettings[String, Array[Byte]], times: mutable.ArrayBuffer[Long],
                          latencyCase: LatencyCase)(using system: ActorSystem[Nothing]):
  Future[Done] =
    given ExecutionContext = system.executionContext
    val kafkaCons = Consumer.plainSource(
      settings,
      Subscriptions.assignmentOffsetsForTimes(
        new TopicPartition("rdf", 0) -> (System.currentTimeMillis - 100)
      )
    )
    val f = kafkaCons
      .map(cr => cr.value())
      .via(deserializer)
      .map(elements => {
        // Enumerate the statements to make sure they are all deserialized
        elements.iterator.foreach(_ => ())
        times.append(System.nanoTime())
      })
      .take(latencyCase.messages)
      .completionTimeout(30.minutes)
      .run()
    f recover {
      case e =>
        println(s"Error in consumer: $e")
        e.printStackTrace()
        Done
    }

  private def runProducer(serializer: Flow[ModelOrDataset, Array[Byte], NotUsed],
                          settings: ProducerSettings[String, Array[Byte]], times: mutable.ArrayBuffer[Long],
                          latencyCase: LatencyCase)(using system: ActorSystem[Nothing]):
  Future[Done] =
    given ExecutionContext = system.executionContext
    val kafkaProd = Producer.plainSink(settings)
    val source = latencyCase.interval match
      case Some(interval) => Source[ModelOrDataset](sourceData.fold(identity, identity))
        .take(latencyCase.messages)
        .grouped(latencyCase.burst.getOrElse(1))
        .zip(Source.tick(0.seconds, interval, 0))
        .mapConcat((el, _) => {
          val time = System.nanoTime()
          for _ <- 1 to el.size do
            times += time
          el
        })
      case None => Source[ModelOrDataset](sourceData.fold(identity, identity))
        .take(latencyCase.messages)
        .map(el => {
          times += System.nanoTime()
          el
        })
    source
      .via(serializer)
      .map(bytes => new ProducerRecord[String, Array[Byte]]("rdf", bytes))
      .runWith(kafkaProd)
