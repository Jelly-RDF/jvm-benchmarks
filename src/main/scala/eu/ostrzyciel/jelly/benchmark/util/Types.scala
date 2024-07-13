package eu.ostrzyciel.jelly.benchmark.util

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.{DatasetGraph, Quad}

type GroupedData = Either[Seq[Model], Seq[DatasetGraph]]
type FlatData = Either[Seq[Triple], Seq[Quad]]
