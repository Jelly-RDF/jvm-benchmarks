package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.*
import org.apache.jena.riot.system.StreamRDFLib
import org.apache.jena.riot.{RDFFormat, RDFParser}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.io.InputStream
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

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
  protected final def desJena(input: InputStream, format: RDFFormat, streamType: String): Unit =
    RDFParser.source(input).lang(format.getLang).parse(StreamRDFLib.sinkNull())
