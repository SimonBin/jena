package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TupleAccessorTripleAnyToNull
    extends TupleAccessorTriple
{
    public static final TupleAccessorTripleAnyToNull INSTANCE = new TupleAccessorTripleAnyToNull();

    @Override
    public Node get(Triple quad, int idx) {
        return getComponent(quad, idx);
    }

    /**
     * Surely there is some common util method somewhere?
     *
     *
     * @return
     */
    public static Node getComponent(Triple quad, int idx) {
        switch(idx) {
        case 0: return TupleAccessorQuadAnyToNull.anyToNull(quad.getSubject());
        case 1: return TupleAccessorQuadAnyToNull.anyToNull(quad.getPredicate());
        case 2: return TupleAccessorQuadAnyToNull.anyToNull(quad.getObject());
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }



}
