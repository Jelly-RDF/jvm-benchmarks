package eu.ostrzyciel.jelly.benchmark.traits

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.core.proto.v1.*
import eu.ostrzyciel.jelly.stream.{DecoderFlow, EncoderFlow}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFWriter
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.*
import org.apache.pekko.NotUsed
import org.apache.pekko.kafka.*
import org.apache.pekko.stream.scaladsl.*

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.jdk.CollectionConverters.*

trait Kafka extends Networked:
  import eu.ostrzyciel.jelly.benchmark.util.Experiments.*
  import eu.ostrzyciel.jelly.convert.jena.given
  
  // Dirty hack: we know that these tests always fail. In the Kafka setup, it's rather painful to
  // abort the test midway, so we just skip them.
  protected final val forbiddenCombinations = Set(
    ("rdf-xml", "yago-annotated-facts"),
    ("json-ld", "yago-annotated-facts")
  )
  
  protected final def isForbiddenCombination(expName: String, dataset: String): Boolean =
    forbiddenCombinations.exists((pExp, pDataset) => expName.contains(pExp) && dataset.contains(pDataset))

  protected final var useGzip: Boolean = _

  protected final val prodSettings: ProducerSettings[String, Array[Byte]] = ProducerSettings(serverSystem, new StringSerializer, new ByteArraySerializer)
    .withProperty(ProducerConfig.RETRIES_CONFIG, "1000000")
    .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")

  protected final val consSettings: ConsumerSettings[String, Array[Byte]] = ConsumerSettings(clientSystem, new StringDeserializer, new ByteArrayDeserializer)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  protected final def getSerializer(experiment: String, config: Config): Flow[ModelOrDataset, Array[Byte], NotUsed] =
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

  protected final def getDeserializer(experiment: String): Flow[Array[Byte], IterableOnce[TripleOrQuad], NotUsed] =
    def inputStream(bytes: Array[Byte]): InputStream =
      var is: InputStream = new ByteArrayInputStream(bytes)
      if useGzip then
        is = new GZIPInputStream(is)
      is

    if experiment.startsWith("jelly") then
      val decoder: Flow[RdfStreamFrame, IterableOnce[TripleOrQuad], NotUsed] =
        if streamType == "triples" then DecoderFlow.decodeTriples.asGraphStream
        else if streamType == "quads" then DecoderFlow.decodeQuads.asDatasetStreamOfQuads
        else DecoderFlow.decodeGraphs.asDatasetStreamOfQuads
      Flow[Array[Byte]]
        .map(bytes => RdfStreamFrame.parseFrom(inputStream(bytes)))
        .via(decoder)
    else
      val jenaLang = getJenaFormat(experiment, streamType).get.getLang
      Flow[Array[Byte]]
        .map(bytes => AsyncParser.asyncParseTriples(inputStream(bytes), jenaLang, "").asScala)

  protected final def recreateTopic(): Unit =
    try {
      val admin = Admin.create(prodSettings.getProperties)
      admin.deleteTopics(Seq("rdf").asJava).all()
        .get(10, java.util.concurrent.TimeUnit.SECONDS)
      admin.createTopics(Seq(NewTopic("rdf", 1, 1.toShort)).asJava).all()
        .get(10, java.util.concurrent.TimeUnit.SECONDS)
      println("Topic recreated")
    } catch {
      case e: Exception =>
        println("Failed to delete topic: " + e.getMessage)
        e.printStackTrace()
    }
