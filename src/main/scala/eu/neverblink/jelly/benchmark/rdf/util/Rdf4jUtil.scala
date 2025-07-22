package eu.neverblink.jelly.benchmark.rdf.util

import org.eclipse.rdf4j.rio.RDFHandler

object Rdf4jUtil:
  class NullRdfHandler extends RDFHandler:
    override def startRDF(): Unit = ()
    override def endRDF(): Unit = ()
    override def handleNamespace(prefix: String, uri: String): Unit = ()
    override def handleStatement(st: org.eclipse.rdf4j.model.Statement): Unit = ()
    override def handleComment(comment: String): Unit = ()
