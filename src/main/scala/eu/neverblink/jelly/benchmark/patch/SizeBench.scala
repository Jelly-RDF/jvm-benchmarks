package eu.neverblink.jelly.benchmark.patch

import eu.neverblink.jelly.benchmark.patch.util.SerDesUtil
import org.apache.commons.io.output.CountingOutputStream
import org.apache.jena.rdfpatch.changes.{RDFChangesCounter, RDFChangesN}

import java.io.OutputStream

object SizeBench:
  @main
  def runPatchSizeBench(): Unit =
    val sinks = Seq(
      "jena-text",
      "jena-binary",
      "jena-sparql-update",
      "jelly"
    ).map(implName => {
      val os = CountingOutputStream(OutputStream.nullOutputStream())
      implName -> (os, SerDesUtil.getWriter(implName, os))
    }).toMap
    val summarySink = RDFChangesCounter()
    val aggregatedSink = RDFChangesN(summarySink +: sinks.values.map(_._2).toSeq *)
    SerDesUtil.readInputDataset(aggregatedSink)
    val sizes = sinks.map { case (implName, (os, _)) =>
      implName -> os.getByteCount
    }

    println("Patch size benchmark results:")
    println(sizes)
    println("Patch summary:")
    val s = summarySink.summary()
    val totalEvents = s.countHeader +
      s.countAddData + s.countDeleteData + s.countAddPrefix + s.countDeletePrefix +
      s.countTxnBegin + s.countTxnCommit + s.countTxnAbort
    println(
      f"""
         |- start: ${s.countStart}
         |- finish: ${s.countFinish}
         |- header: ${s.countHeader}
         |- addData: ${s.countAddData}
         |- deleteData: ${s.countDeleteData}
         |- addPrefix: ${s.countAddPrefix}
         |- deletePrefix: ${s.countDeletePrefix}
         |- txnBegin: ${s.countTxnBegin}
         |- txnCommit: ${s.countTxnCommit}
         |- txnAbort: ${s.countTxnAbort}
         |- segment: ${s.countSegment}
         |
         |- total events: $totalEvents
         |
         |""".stripMargin)
