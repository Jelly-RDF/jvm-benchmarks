package eu.ostrzyciel.jelly.benchmark.traits

import com.typesafe.config.Config
import eu.ostrzyciel.jelly.benchmark.util.*
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.Sink

import scala.concurrent.Future

trait Networked:
  protected val config: Config = ConfigManager.config

  protected val serverSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamServer", config)
  protected val clientSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "StreamClient", config)

  protected var experiments: Seq[String] = _
  protected var streamType: String = _
  protected var numElements: Long = _
  protected var numStatements: Long = _
  protected var sourceData: GroupedData = _
  protected var sourceFilePath: String = _

  protected final def initExperiments(streamType: String, useJena: Boolean): Unit =
    experiments = Experiments.getFormatKeysToTest(
      jena = useJena, jenaStreaming = useJena, jelly = true, rdf4j = false,
      streamType = streamType
    )
    this.streamType = streamType

  protected final def loadData(path: String, streamType: String, elements: Int): Unit =
    val d = DataLoader.getSourceData(
      path, streamType, 0, if elements == 0 then None else Some(elements)
    )(using serverSystem)
    numStatements = d._1
    numElements = d._2
    sourceData = d._3
    sourceFilePath = path

  protected final def countSink[T]: Sink[IterableOnce[T], Future[(Long, Long)]] =
    Sink.fold((0L, 0L))((counter, els) =>
      (counter._1 + els.iterator.size, counter._2 + 1)
    )
