package eu.neverblink.jelly.benchmark.patch

import eu.neverblink.jelly.benchmark.patch.util.*
import eu.neverblink.jelly.benchmark.util.ConfigManager
import eu.neverblink.jelly.convert.jena.patch.*
import eu.neverblink.jelly.core.proto.v1.patch.PatchStatementType
import org.apache.jena.atlas.io.Writer2
import org.apache.jena.rdfpatch.RDFChanges
import org.apache.jena.rdfpatch.binary.RDFChangesWriterBinary
import org.apache.jena.rdfpatch.changes.RDFChangesWriteUpdate
import org.apache.jena.rdfpatch.text.RDFChangesWriterText
import org.apache.jena.riot.thrift.TRDF
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.io.{FileInputStream, OutputStreamWriter}
import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized
import scala.util.Using

/**
 * Serialization speed benchmark for Jena RDF Patch implementations.
 *
 * Requires the following config options to be set:
 *
 * - `jelly.patch.input-file`
 * - `jelly.patch.statement-type`
 */
object SerBench:
  @State(Scope.Benchmark)
  class BenchInput:
    var jenaChangesCollector: JenaChangesCollector = uninitialized

    @Param(Array("jelly"/*, "jena-text", "jena-binary", "jena-sparql-update"*/))
    var implementation: String = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      val inputFile = java.io.File(ConfigManager.patch.inputFile)
      val statementType = if ConfigManager.patch.statementType.toLowerCase == "triples" then
        PatchStatementType.TRIPLES
      else if ConfigManager.patch.statementType.toLowerCase == "quads" then
        PatchStatementType.QUADS
      else
        throw new IllegalArgumentException(s"Unknown statement type: ${ConfigManager.patch.statementType}")
      jenaChangesCollector = JellyPatchOps.changesCollector(statementType)

      println(f"Reading patch dataset from ${inputFile.getAbsolutePath} ...")
      Using.resource(FileInputStream(inputFile)) { fis =>
        val reader = RdfPatchReaderJelly(
          RdfPatchReaderJelly.Options(),
          JenaPatchConverterFactory.getInstance(),
          fis
        )
        reader.apply(jenaChangesCollector)
      }
      println(f"Read ${jenaChangesCollector.size} changes from the patch dataset.")

    def getWriter(blackhole: Blackhole): RDFChanges = implementation match
      case "jelly" => RdfPatchWriterJelly(
        RdfPatchWriterJelly.Options(),
        JenaPatchConverterFactory.getInstance(),
        BlackholeOutputStream(blackhole)
      )
      case "jena-text" => RDFChangesWriterText.create(
        BlackholeOutputStream(blackhole)
      )
      case "jena-binary" =>
        val protocol = TRDF.protocol(BlackholeOutputStream(blackhole))
        RDFChangesWriterBinary(protocol)
      case "jena-sparql-update" =>
        val writer = OutputStreamWriter(BlackholeOutputStream(blackhole))
        val writer2 = Writer2.wrapNoBuffer(writer)
        RDFChangesWriteUpdate(writer2)
      case _ =>
        throw new IllegalArgumentException(s"Unknown implementation: $implementation")


class SerBench:
  import SerBench.*

  @Benchmark
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def jelly(blackhole: Blackhole, input: BenchInput): Unit =
    val writer = input.getWriter(blackhole)
    input.jenaChangesCollector.replay(writer, true)
