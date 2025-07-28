package eu.neverblink.jelly.benchmark.traits;

import eu.neverblink.jelly.convert.jena.patch.JenaChangesCollector;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFWriter;

/**
 * Helper methods for iterating over triples and quads, because scalac refuses to generate sensible
 * bytecode for the equivalent for loops.
 */
public class FlatSerDesHelper {
    public static void serJenaTriples(Triple[] triples, StreamRDF writer) {
        for (Triple triple : triples) {
            writer.triple(triple);
        }
    }
    
    public static void serJenaQuads(Quad[] quads, StreamRDF writer) {
        for (Quad quad : quads) {
            writer.quad(quad);
        }
    }
    
    public static void serRdf4j(Statement[] statements, RDFWriter writer) {
        for (Statement statement : statements) {
            writer.handleStatement(statement);
        }
    }
}
