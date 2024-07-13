package eu.ostrzyciel.jelly.benchmark.util

import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.DatasetGraph

type BenchmarkData = Either[Seq[Model], Seq[DatasetGraph]]
