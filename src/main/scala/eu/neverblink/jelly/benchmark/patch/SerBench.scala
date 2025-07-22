package eu.neverblink.jelly.benchmark.patch

import eu.neverblink.jelly.benchmark.util.ConfigManager
import org.openjdk.jmh.annotations.*

import java.io.FileInputStream
import scala.util.Using

object SerBench:
  @State(Scope.Benchmark)
  class BenchInput:
    var toSerialize: Array[Object] = _

    @Setup(Level.Trial)
    def setup(): Unit =
      val inputFile = java.io.File(ConfigManager.patch.inputFile)
      Using.resource(FileInputStream(inputFile)) { fis =>
        
      }
      

class SerBench:
  ???