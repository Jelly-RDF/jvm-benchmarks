package eu.ostrzyciel.jelly.benchmark.util

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.{DatasetGraph, Quad}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.eclipse.rdf4j.model.Statement

type GroupedData = Either[Seq[Model], Seq[DatasetGraph]]
type GroupedDataRdf4j = Seq[Seq[Statement]]
type FlatData = Either[Array[Triple], Array[Quad]]
type FlatDataRdf4j = Array[Statement]
type GroupedDataStream = Either[Source[Model, NotUsed], Source[DatasetGraph, NotUsed]]
type GroupedDataStreamRdf4j = Source[Seq[Statement], NotUsed]
type FlatDataStream = Either[Source[Triple, NotUsed], Source[Quad, NotUsed]]
type FlatDataStreamRdf4j = Source[Statement, NotUsed]

type TripleOrQuad = Triple | Quad
type ModelOrDataset = Model | DatasetGraph