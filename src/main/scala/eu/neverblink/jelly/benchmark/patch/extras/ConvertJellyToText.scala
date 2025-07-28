package eu.neverblink.jelly.benchmark.patch.extras

import eu.neverblink.jelly.convert.jena.patch.{JenaPatchConverterFactory, RdfPatchReaderJelly}
import org.apache.jena.rdfpatch.text.RDFChangesWriterText

object ConvertJellyToText:

  /**
   * Converts a Jelly-Patch file to a text file in the RDF Patch format.
   *
   * @param inputFile the input Jelly-Patch file to convert (.jellyp)
   * @param outputFile the output file where the text representation will be written (.rdfp)
   */
  @main
  def runConvertJellyToText(inputFile: String, outputFile: String): Unit =
    val input = java.io.File(inputFile)
    val reader = RdfPatchReaderJelly(
      RdfPatchReaderJelly.Options(),
      JenaPatchConverterFactory.getInstance(),
      new java.io.FileInputStream(input)
    )
    val writer = RDFChangesWriterText.create(
      new java.io.FileOutputStream(outputFile)
    )
    println("Converting...")
    reader.apply(writer)
