package eu.neverblink.jelly.benchmark.patch.util

import eu.neverblink.jelly.benchmark.util.ConfigManager
import eu.neverblink.jelly.convert.jena.patch.*
import eu.neverblink.jelly.core.proto.v1.patch.PatchStatementType
import org.apache.jena.atlas.io.Writer2
import org.apache.jena.rdfpatch.RDFChanges
import org.apache.jena.rdfpatch.binary.RDFChangesWriterBinary
import org.apache.jena.rdfpatch.changes.{RDFChangesWrapper, RDFChangesWriteUpdate}
import org.apache.jena.rdfpatch.text.RDFChangesWriterText
import org.apache.jena.riot.thrift.TRDF

import java.io.{FileInputStream, OutputStream, OutputStreamWriter}
import scala.util.Using

object SerDesUtil:
  private case object MaxSegmentsReachedException extends Exception

  def getWriter(implementation: String, output: OutputStream): RDFChanges =
    implementation match
      case "jelly" => RdfPatchWriterJelly(
        RdfPatchWriterJelly.Options(),
        JenaPatchConverterFactory.getInstance(),
        output
      )
      case "jena-text" => RDFChangesWriterText.create(output)
      case "jena-binary" =>
        val protocol = TRDF.protocol(output)
        RDFChangesWriterBinary(protocol)
      case "jena-sparql-update" =>
        val writer = OutputStreamWriter(output)
        val writer2 = Writer2.wrapNoBuffer(writer)
        RDFChangesWriteUpdate(writer2)
      case _ =>
        throw new IllegalArgumentException(s"Unknown implementation: $implementation")

  def getStatementType: PatchStatementType =
    if ConfigManager.patch.statementType.toLowerCase == "triples" then
      PatchStatementType.TRIPLES
    else if ConfigManager.patch.statementType.toLowerCase == "quads" then
      PatchStatementType.QUADS
    else
      throw new IllegalArgumentException(s"Unknown statement type: ${ConfigManager.patch.statementType}")


  def readInputDataset(dest: RDFChanges): Unit =
    val inputFile = java.io.File(ConfigManager.patch.inputFile)
    val maxSegments = ConfigManager.patch.maxSegments
    var segmentCount = 0
    val wrapper = new RDFChangesWrapper(dest) {
      override def segment(): Unit =
        super.segment()
        segmentCount += 1
        if segmentCount >= maxSegments then
          throw MaxSegmentsReachedException
    }
    println(f"Reading patch dataset from ${inputFile.getAbsolutePath} ...")
    Using.resource(FileInputStream(inputFile)) { fis =>
      val reader = RdfPatchReaderJelly(
        RdfPatchReaderJelly.Options(),
        JenaPatchConverterFactory.getInstance(),
        fis
      )
      try {
        reader.apply(wrapper)
      } catch {
        case MaxSegmentsReachedException =>
          println(f"Max segments reached: $maxSegments, finishing")
        case t => throw t
      }
      println(f"Read $segmentCount segments from the patch dataset.")
    }
