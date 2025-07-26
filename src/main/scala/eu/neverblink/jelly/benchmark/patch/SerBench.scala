package eu.neverblink.jelly.benchmark.patch

import eu.neverblink.jelly.benchmark.patch.util.*
import eu.neverblink.jelly.convert.jena.patch.*
import org.apache.jena.rdfpatch.RDFChanges
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized

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

    @Param(Array("jelly", "jena-binary" , "jena-text", "jena-sparql-update"))
    var implementation: String = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      jenaChangesCollector = JellyPatchOps.changesCollector(SerDesUtil.getStatementType)
      SerDesUtil.readInputDataset(jenaChangesCollector)
      println(f"Read ${jenaChangesCollector.size} changes from the patch dataset.")

    def getWriter(blackhole: Blackhole): RDFChanges =
      SerDesUtil.getWriter(implementation, BlackholeOutputStream(blackhole))


class SerBench:
  import SerBench.*

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def serialize(blackhole: Blackhole, input: BenchInput): Unit =
    val writer = input.getWriter(blackhole)
    input.jenaChangesCollector.replay(writer, true)
