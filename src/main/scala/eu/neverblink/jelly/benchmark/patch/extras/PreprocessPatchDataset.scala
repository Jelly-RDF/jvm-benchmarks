package eu.neverblink.jelly.benchmark.patch.extras

import eu.neverblink.jelly.convert.jena.patch.*
import eu.neverblink.jelly.core.patch.JellyPatchOptions
import eu.neverblink.jelly.core.proto.v1.patch.*
import org.apache.jena.rdfpatch.changes.RDFChangesWrapper
import org.apache.jena.rdfpatch.{RDFChanges, RDFPatchOps}

import java.io.{File, FileInputStream, FileOutputStream}
import scala.jdk.CollectionConverters.*
import scala.util.Using

object PreprocessPatchDataset:
  /**
   * Preprocesses a patch dataset into a Jelly-Patch file.
   *
   * This is currently a few hard-coded modes for specific input formats:
   *
   * - `jena-log`: Reads Jena log files (patches) and converts them to Jelly-Patch format.
   * - `jelly-multiple`: Reads a Jelly-Patch file that is a concatenation of multiple Jelly-Patch
   *   streams and merges them into one file.
   * - `jelly-resplit`: Reads a Jelly-Patch file that does not have delimiters between patches and
   *    resplits it along transaction boundaries.
   *
   * @param input input directory or file containing the patch dataset.
   * @param outputFile output file where the preprocessed Jelly-Patch will be written.
   * @param preprocessingMode the mode of preprocessing to apply.
   */
  @main
  def runPreprocessPatchDataset(input: String, outputFile: String, preprocessingMode: String): Unit =
    Using.resource(FileOutputStream(File(outputFile))) { fos =>
      val writer = RdfPatchWriterJelly(
        RdfPatchWriterJelly.Options(),
        JenaPatchConverterFactory.getInstance(),
        fos
      )
      if preprocessingMode == "jena-log" then
        preprocessJenaLog(input, writer)
      else if preprocessingMode == "jelly-multiple" then
        preprocessJellyMultiple(input, writer)
      else if preprocessingMode == "jelly-resplit" then
        preprocessJellyResplit(input, writer)
      else
        throw new IllegalArgumentException(s"Unknown preprocessing mode: $preprocessingMode")
    }

  private def preprocessJenaLog(inputDir: String, dest: RDFChanges): Unit =
    // list files in inputDir
    val inputFiles = new java.io.File(inputDir).listFiles
      .filter(_.isFile)
      .filter(_.getName.startsWith("patch-"))
      .sortBy(_.getName.split("-").last.toInt)

    println(f"Found ${inputFiles.length} files in $inputDir")
    println(f"First file: ${inputFiles.head.getName}")
    println(f"Last file: ${inputFiles.last.getName}")
    println(f"Converting...")

    inputFiles
      .foreach { file =>
        Using.resource(FileInputStream(file)) { fis =>
//          println(f"\n\n\n# Processing file: ${file.getName}\n")
          RDFPatchOps.read(fis).apply(dest)
          dest.segment()
        }
      }

  private def preprocessJellyMultiple(inputFile: String, dest: RDFChanges): Unit =
    println(f"Converting...")
    Using.resource(FileInputStream(File(inputFile))) { fis =>
      // Jelly-Patch parser that can handle frames encoded independently of each other.
      // It will not work for actually correct Jelly-Patch files.
      val inHandler = JellyPatchOps.fromJellyToJena(dest)
      var frame: RdfPatchFrame = null
      Iterator.continually(RdfPatchFrame.parseDelimitedFrom(fis))
        .takeWhile(_ != null)
        .foreach { frame =>
          frame.getRows.asScala.head.getOptions.asInstanceOf[RdfPatchOptions.Mutable]
            .setStatementType(PatchStatementType.TRIPLES)
            .setStreamType(PatchStreamType.FRAME)
          val decoder = JenaPatchConverterFactory.getInstance().anyStatementDecoder(
            inHandler, JellyPatchOptions.DEFAULT_SUPPORTED_OPTIONS
          )
          // println(frame)
          decoder.ingestFrame(frame)
        }
    }

  private def preprocessJellyResplit(inputFile: String, dest: RDFChanges): Unit =
    println(f"Converting...")
    Using.resource(FileInputStream(File(inputFile))) { fis =>
      val handler = new RDFChangesWrapper(dest) {
        override def txnCommit(): Unit = {
          super.txnCommit()
          segment()
        }

        override def txnAbort(): Unit = {
          super.txnAbort()
          segment()
        }
      }
      RdfPatchReaderJelly(
        RdfPatchReaderJelly.Options(),
        JenaPatchConverterFactory.getInstance(),
        fis
      ).apply(handler)
    }
