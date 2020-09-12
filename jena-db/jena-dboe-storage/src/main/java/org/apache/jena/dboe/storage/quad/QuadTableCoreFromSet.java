package org.apache.jena.dboe.storage.quad;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.tuple.TupleTableCoreFromSet;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;


/**
 * Basic Quad T
 *
 * @author raven
 *
 */
public class QuadTableCoreFromSet
    extends TupleTableCoreFromSet<Quad, Node>
    implements QuadTableCore
{
    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        return find()
                .filter(quad -> quad.matches(g, s, p, o));
    }

    @Override
    public Stream<Node> listGraphNodes() {
        return find().map(Quad::getGraph).distinct();
    }
}
