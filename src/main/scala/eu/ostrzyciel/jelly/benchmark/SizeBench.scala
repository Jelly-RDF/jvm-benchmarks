package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.benchmark.util.DataLoader
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamRow}
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph

import java.io.{ByteArrayOutputStream, OutputStream}
import scala.collection.mutable
import scala.util.Random

object SizeBench extends SerDesBench:
  import Experiments.*
  import Util.*

  private val sizes: mutable.Map[String, Long] = mutable.Map.empty

  private val zeroIndices: mutable.Map[Long, (Long, Long, Long)] = mutable.Map.empty
  private val nonZeroIndices: mutable.Map[Long, (Long, Long, Long)] = mutable.Map.empty

  private val zeroReferences: mutable.Map[Long, (Long, Long)] = mutable.Map.empty
  private val nonZeroReferences: mutable.Map[Long, (Long, Long)] = mutable.Map.empty

  // Arguments: [triples/graphs/quads] [element size] [source file path]
  def main(args: Array[String]): Unit =
    val streamType = args(0)
    val elementSize = args(1).toInt
    val filePath = args(2)
    // Only run Jelly for GRAPHS streams – in Jena it's the same as QUADS
    val experiments2 = (if streamType != "graphs" then experiments else Random.shuffle(jellyOptions.keys)).toSeq

    val (numStatements, numElements, data) = DataLoader.getSourceData(filePath, streamType, elementSize)
    run(data, experiments2, streamType)

    saveRunInfo(s"size_${args(0)}", conf, Map(
      "elements" -> numElements,
      "statements" -> numStatements,
      "order" -> experiments,
      "sizes" -> sizes,
      "file" -> filePath,
      "elementSize" -> elementSize,
      "streamType" -> streamType,
      "zeroLookupIndices" -> zeroIndices.toSeq.sortBy(_._1).map(_._2),
      "nonZeroLookupIndices" -> nonZeroIndices.toSeq.sortBy(_._1).map(_._2),
      "zeroLookupReferences" -> zeroReferences.toSeq.sortBy(_._1).map(_._2),
      "nonZeroLookupReferences" -> nonZeroReferences.toSeq.sortBy(_._1).map(_._2),
    ))
    sys.exit()
    
    
  def updateDetailedStats(frame: RdfStreamFrame, i: Long): Unit =
    val key = i / 10000L
    // Update the counts of zero and non-zero indices in lookup table entries
    val zeros = zeroIndices.getOrElse(key, (0L, 0L, 0L))
    zeroIndices.update(key, (
      zeros._1 + frame.rows.count(r => r.row.isPrefix && r.row.prefix.get.id == 0),
      zeros._2 + frame.rows.count(r => r.row.isName && r.row.name.get.id == 0),
      zeros._3 + frame.rows.count(r => r.row.isDatatype && r.row.datatype.get.id == 0),
    ))
    val nonZeros = nonZeroIndices.getOrElse(key, (0L, 0L, 0L))
    nonZeroIndices.update(key, (
      nonZeros._1 + frame.rows.count(r => r.row.isPrefix && r.row.prefix.get.id != 0),
      nonZeros._2 + frame.rows.count(r => r.row.isName && r.row.name.get.id != 0),
      nonZeros._3 + frame.rows.count(r => r.row.isDatatype && r.row.datatype.get.id != 0),
    ))

    // Update the counts of zero and non-zero references to lookup table entries in IRIs
    val iris = frame.rows
      .map(_.row)
      .collect {
        case r: RdfStreamRow.Row.Triple =>
          Seq(r.value.subject.sIri, r.value.predicate.pIri, r.value.`object`.oIri).flatten
        case r: RdfStreamRow.Row.Quad =>
          Seq(r.value.subject.sIri, r.value.predicate.pIri, r.value.`object`.oIri, r.value.graph.gIri).flatten
      }
      .flatten
    val zeroRefs = zeroReferences.getOrElse(key, (0L, 0L))
    zeroReferences.update(key, (
      zeroRefs._1 + iris.count(_.prefixId == 0),
      zeroRefs._2 + iris.count(_.nameId == 0),
    ))
    val nonZeroRefs = nonZeroReferences.getOrElse(key, (0L, 0L))
    nonZeroReferences.update(key, (
      nonZeroRefs._1 + iris.count(_.prefixId != 0),
      nonZeroRefs._2 + iris.count(_.nameId != 0),
    ))

  
  def run(data: Either[Seq[Model], Seq[DatasetGraph]], exps: Seq[String], streamType: String): Unit =
    for gzip <- Seq(false, true); experiment <- exps do
      System.gc()
      println("Sleeping 3 seconds...")
      Thread.sleep(3000)
      println(f"Running experiment $experiment with gzip $gzip")

      val expName = if gzip then s"$experiment-gzip" else experiment
      sizes.getOrElseUpdate(expName, 0L)

      def getOs: (OutputStream, ByteArrayOutputStream) =
        val baos = new ByteArrayOutputStream()
        if gzip then
          (new java.util.zip.GZIPOutputStream(baos), baos)
        else (baos, baos)

      if experiment.startsWith("jelly") then
        var i = 0L
        serJelly(data, getJellyOpts(experiment, streamType, false), frame => {
          val (os, baos) = getOs
          frame.writeTo(os)
          os.close()
          sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))
          
          if !gzip then
            updateDetailedStats(frame, i)
          
          i += 1
        })
      else
        val sourceFlat = data match
          case Left(v) => v
          case Right(v) => v
        for item <- sourceFlat do
          val (os, baos) = getOs
          serJena(item, getFormat(experiment, streamType), os)
          os.close()
          sizes.updateWith(expName)(_.map(_ + baos.size()).orElse(Some(baos.size())))
