package eu.ostrzyciel.jelly.benchmark.traits

import com.typesafe.config.ConfigFactory
import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.system.AsyncParser
import org.apache.jena.riot.{RDFFormat, RDFWriter}
import org.apache.jena.sparql.core.DatasetGraph
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.{InputStream, OutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.Random

trait SerDes:
  protected given system: ActorSystem[Nothing] = ActorSystem(
    Behaviors.empty, "SerDesBench", ConfigManager.config
  )
  protected given ExecutionContext = system.executionContext

  protected var experiments: Seq[String] = _
  protected var times: Map[String, mutable.ArrayBuffer[Long]] = _
  protected var streamType: String = _

  protected final def initExperiment(flatStreaming: Boolean, streamType: String): Unit =
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val doJena = streamType != "graphs"
    this.streamType = streamType
    experiments = Experiments.getFormatKeysToTest(
      jena = doJena && !flatStreaming, 
      jenaStreaming = doJena, 
      jelly = true, 
      streamType
    )
    times = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap

  /**
   * This is the same for grouped and flat benchmarks. In Jena all parsers have a
   * statement-level streaming interface.
   */
  protected final def desJena(input: InputStream, format: RDFFormat, streamType: String): Unit = (
    if streamType == "triples" then AsyncParser.asyncParseTriples(input, format.getLang, "")
    else AsyncParser.asyncParseQuads(input, format.getLang, "")
  ).forEachRemaining(_ => {})