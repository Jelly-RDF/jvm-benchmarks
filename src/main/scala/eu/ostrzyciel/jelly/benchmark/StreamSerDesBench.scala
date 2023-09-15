package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamOptions, RdfStreamType}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFFormat
import org.apache.jena.sparql.core.DatasetGraph

object StreamSerDesBench extends SerDesBench:
  import Util.*

  // Arguments: [ser/des] [triples/graphs/quads] [source file path]
  def main(args: Array[String]): Unit =
    val task = args(0)
    val streamType = args(1)
    val filePath = args(2)

    val data = getSourceData(filePath, streamType)

    if task == "ser" then
      ()
      // mainSer(streamType, data)
    else if task == "des" then
      ()
      // mainDes(streamType, data)

    printSpeed(numStatements, times)
    saveRunInfo(s"stream_raw_${args(0)}", conf, Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "times" -> times,
      "file" -> filePath,
      "task" -> task,
      "streamType" -> streamType,
    ))
    sys.exit()

  private def getFormat(exp: String, streamType: String): RDFFormat =
    val tuple = jenaFormats(exp)
    if streamType == "triples" then tuple(0) else tuple(1)

  private def getJellyOpts(exp: String, streamType: String): RdfStreamOptions =
    jellyOptions(exp).withStreamType(
      streamType match
        case "triples" => RdfStreamType.RDF_STREAM_TYPE_TRIPLES
        case "graphs" => RdfStreamType.RDF_STREAM_TYPE_GRAPHS
        case "quads" => RdfStreamType.RDF_STREAM_TYPE_QUADS
    )

  private def getSourceData(path: String, streamType: String): Either[Seq[Model], Seq[DatasetGraph]] =
    ???

