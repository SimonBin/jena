package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class TupleAccessorQuadAnyToNull
    extends TupleAccessorQuad
{
    public static final TupleAccessorQuadAnyToNull INSTANCE = new TupleAccessorQuadAnyToNull();

    @Override
    public Node get(Quad quad, int idx) {
        return getComponent(quad, idx);
    }


    /**
     * Surely there is some common util method somewhere?
     *
     *
     * @return
     */
    public static Node getComponent(Quad quad, int idx) {
        switch(idx) {
        case 0: return anyToNull(quad.getSubject());
        case 1: return anyToNull(quad.getPredicate());
        case 2: return anyToNull(quad.getObject());
        case 3: return anyToNull(quad.getGraph());
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }

    public static Node anyToNull(Node n) {
        return Node.ANY.equals(n) ? null : n;
    }


}
