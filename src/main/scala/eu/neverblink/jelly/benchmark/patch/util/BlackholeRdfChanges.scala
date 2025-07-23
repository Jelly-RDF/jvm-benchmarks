package eu.neverblink.jelly.benchmark.patch.util

import org.apache.jena.graph.Node
import org.apache.jena.rdfpatch.RDFChanges
import org.openjdk.jmh.infra.Blackhole

final class BlackholeRdfChanges(blackhole: Blackhole) extends RDFChanges:

  def header(field: String, value: Node): Unit =
    blackhole.consume(field)
    blackhole.consume(value)

  def add(g: Node, s: Node, p: Node, o: Node): Unit =
    blackhole.consume(g)
    blackhole.consume(s)
    blackhole.consume(p)
    blackhole.consume(o)

  def delete(g: Node, s: Node, p: Node, o: Node): Unit =
    blackhole.consume(g)
    blackhole.consume(s)
    blackhole.consume(p)
    blackhole.consume(o)

  def addPrefix(gn: Node, prefix: String, uriStr: String): Unit =
    blackhole.consume(gn)
    blackhole.consume(prefix)
    blackhole.consume(uriStr)

  def deletePrefix(gn: Node, prefix: String): Unit =
    blackhole.consume(gn)
    blackhole.consume(prefix)

  def txnBegin(): Unit = blackhole.consume(null)

  def txnCommit(): Unit = blackhole.consume(null)

  def txnAbort(): Unit = blackhole.consume(null)

  def segment(): Unit = blackhole.consume(null)

  def start(): Unit = blackhole.consume(null)

  def finish(): Unit = blackhole.consume(null)
