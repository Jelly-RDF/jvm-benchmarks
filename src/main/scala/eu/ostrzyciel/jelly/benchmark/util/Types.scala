package eu.ostrzyciel.jelly.benchmark.util

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.{DatasetGraph, Quad}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

type GroupedData = Either[Seq[Model], Seq[DatasetGraph]]
type FlatData = Either[Seq[Triple], Seq[Quad]]
type GroupedDataStream = Either[Source[Model, NotUsed], Source[DatasetGraph, NotUsed]]

type TripleOrQuad = Triple | Quad
type ModelOrDataset = Model | DatasetGraph