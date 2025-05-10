//package eu.neverblink.jelly.benchmark.traits
//
//import com.typesafe.config.Config
//import eu.neverblink.jelly.benchmark.util.{ConfigManager, DataLoader, Experiments, GroupedData}
//import eu.neverblink.jelly.benchmark.util.*
//import org.apache.pekko.actor.typed.ActorSystem
//import org.apache.pekko.actor.typed.scaladsl.Behaviors
//import org.apache.pekko.stream.scaladsl.Sink
//
//import scala.compiletime.uninitialized
//import scala.concurrent.Future
//
//trait Networked:
//  protected val config: Config = ConfigManager.config
//
//  protected val serverSystem: ActorSystem[?] = ActorSystem(Behaviors.empty, "StreamServer", config)
//  protected val clientSystem: ActorSystem[?] = ActorSystem(Behaviors.empty, "StreamClient", config)
//
//  protected var experiments: Seq[String] = uninitialized
//  protected var streamType: String = uninitialized
//  protected var numElements: Long = uninitialized
//  protected var numStatements: Long = uninitialized
//  protected var sourceData: GroupedData = uninitialized
//  protected var sourceFilePath: String = uninitialized
//
//  protected final def initExperiments(streamType: String, useJena: Boolean): Unit =
//    experiments = Experiments.getFormatKeysToTest(
//      jena = useJena, jenaStreaming = useJena, jelly = true, rdf4j = false,
//      streamType = streamType
//    )
//    this.streamType = streamType
//
//  protected final def loadData(path: String, streamType: String, elements: Int): Unit =
//    val d = DataLoader.getSourceData(
//      path, streamType, 0, if elements == 0 then None else Some(elements)
//    )(using serverSystem)
//    numStatements = d._1
//    numElements = d._2
//    sourceData = d._3
//    sourceFilePath = path
//
//  protected final def countSink[T]: Sink[IterableOnce[T], Future[(Long, Long)]] =
//    Sink.fold((0L, 0L))((counter, els) =>
//      (counter._1 + els.iterator.size, counter._2 + 1)
//    )
