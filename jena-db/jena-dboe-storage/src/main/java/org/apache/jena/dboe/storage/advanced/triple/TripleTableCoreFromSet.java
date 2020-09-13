package org.apache.jena.dboe.storage.advanced.triple;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableCoreFromSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleTableCoreFromSet
    extends TupleTableCoreFromSet<Triple, Node>
    implements TripleTableCore
{
    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
        Triple pattern = new Triple(s, p, o);
        return findTuples().filter(t -> pattern.matches(t));
    }
}