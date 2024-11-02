package eu.ostrzyciel.jelly.benchmark.traits

import eu.ostrzyciel.jelly.benchmark.util.*
import eu.ostrzyciel.jelly.benchmark.util.Rdf4jUtil.NullRdfHandler
import org.apache.jena.riot.system.StreamRDFLib
import org.apache.jena.riot
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.eclipse.rdf4j.rio

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

  protected final def initExperiment(flatStreaming: Boolean, rdf4j: Boolean, streamType: String): Unit =
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val doJena = streamType != "graphs"
    this.streamType = streamType
    experiments = Experiments.getFormatKeysToTest(
      jena = doJena && !flatStreaming, 
      jenaStreaming = doJena, 
      jelly = true, 
      rdf4j = rdf4j,
      streamType
    )
    times = experiments.map(_ -> mutable.ArrayBuffer[Long]()).toMap

  /**
   * This is the same for grouped and flat benchmarks. In Jena all parsers have a
   * statement-level streaming interface.
   */
  protected final def desJena(input: InputStream, format: riot.RDFFormat, streamType: String): Unit =
    riot.RDFParser.source(input).lang(format.getLang).parse(StreamRDFLib.sinkNull())
    
  protected final def desRdf4j(input: InputStream, format: rio.RDFFormat): Unit =
    val parser = rio.Rio.createParser(format)
    parser.setRDFHandler(NullRdfHandler())
    parser.parse(input, "")
