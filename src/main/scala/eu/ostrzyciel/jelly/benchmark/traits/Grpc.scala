package eu.ostrzyciel.jelly.benchmark.traits

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.benchmark.util.*
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

trait Grpc:
  protected val config: Config = ConfigManager.config

  protected val serverSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamServer", config)
  protected val clientSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamClient", config)

  protected var experiments: Seq[String] = _
  protected var streamType: String = _
  protected var numElements: Long = _
  protected var numStatements: Long = _
  protected var sourceData: GroupedData = _
  protected var sourceFilePath: String = _

  protected final def initExperiments(streamType: String): Unit =
    experiments = Experiments.getFormatKeysToTest(
      jena = false, jenaStreaming = false, jelly = true,
      streamType = streamType
    )
    this.streamType = streamType

  protected final def loadData(path: String, streamType: String): Unit =
    val d = DataLoader.getSourceData(path, streamType, 0)(using serverSystem)
    numStatements = d._1
    numElements = d._2
    sourceData = d._3
    sourceFilePath = path
