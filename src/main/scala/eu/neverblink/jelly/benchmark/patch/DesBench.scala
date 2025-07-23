package eu.neverblink.jelly.benchmark.patch

import eu.neverblink.jelly.benchmark.patch.util.{BlackholeRdfChanges, SerDesUtil}
import eu.neverblink.jelly.convert.jena.patch.*
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.rdfpatch.PatchProcessor
import org.apache.jena.rdfpatch.binary.RDFPatchReaderBinary
import org.apache.jena.rdfpatch.text.RDFPatchReaderText
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized

object DesBench:
  @State(Scope.Benchmark)
  class BenchInput:
    @Param(Array("jelly", "jena-text", "jena-binary"))
    var implementation: String = uninitialized

    var serializedData: Array[Byte] = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      // Output stream for serialization
      val os = ByteArrayOutputStream()
      val writer = SerDesUtil.getWriter(implementation, os)
      SerDesUtil.readInputDataset(
        writer,
        // 2 GiB limit, a bit less than Int.MaxValue to avoid issues with array size
        () => os.size() < 2 * 1000 * 1000 * 1000
      )
      if os.size() < 0 then
        throw new IllegalStateException("Serialized data is larger than Int.MaxValue. " +
          "Reduce benchmark size with `max-segments` config option.")
      serializedData = os.toByteArray

    def getReader: PatchProcessor =
      val input = ByteArrayInputStream(serializedData)
      implementation match
        case "jelly" => RdfPatchReaderJelly(
          RdfPatchReaderJelly.Options(),
          JenaPatchConverterFactory.getInstance(),
          input,
        )
        case "jena-text" => RDFPatchReaderText(input)
        case "jena-binary" => RDFPatchReaderBinary.create(input)
        case _ =>
          throw new IllegalArgumentException(s"Unknown implementation: $implementation")


class DesBench:
  import DesBench.*

  @Benchmark
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def deserialize(blackhole: Blackhole, input: BenchInput): Unit =
    val reader = input.getReader
    val output = BlackholeRdfChanges(blackhole)
    reader.apply(output)
