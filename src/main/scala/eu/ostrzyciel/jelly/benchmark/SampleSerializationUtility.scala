package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.util.DataLoader
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph

import java.io.{ByteArrayOutputStream, FileOutputStream, OutputStream}
import scala.collection.mutable
import scala.util.Random

object SampleSerializationUtility extends SerDesBench:
  import Experiments.*
  import Util.*

  // Arguments: [triples/graphs/quads] [element size] [stream item to serialize] [source file path]
  def main(args: Array[String]): Unit =
    val streamType = args(0)
    val elementSize = args(1).toInt
    val length = args(2).toInt
    val filePath = args(3)

    val (numStatements, numElements, data) = DataLoader.getSourceData(filePath, streamType, elementSize)
    run(data.map(_.take(length)).left.map(_.take(length)), streamType)

    sys.exit()

  def run(data: Either[Seq[Model], Seq[DatasetGraph]], streamType: String): Unit =
    val exps = Experiments.jellyOptions.keys

    var i = 1
    for experiment <- exps do
      println(f"Running experiment $experiment")

      serJelly(data, getJellyOpts(experiment, streamType, false), frame => {
        val os = new FileOutputStream(f"samples/$experiment.jelly")
        frame.writeTo(os)
        os.close()
        i += 1
      })
