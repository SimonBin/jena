package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class TupleAccessorQuad
    implements TupleAccessor<Quad, Node>
{
    public static final TupleAccessorQuad INSTANCE = new TupleAccessorQuad();

    @Override
    public int getRank() {
        return 4;
    }

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
        case 0: return quad.getSubject();
        case 1: return quad.getPredicate();
        case 2: return quad.getObject();
        case 3: return quad.getGraph();
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }

    @Override
    public <T> Quad restore(T obj, TupleAccessor<? super T, ? extends Node> accessor) {
//        if (accessor.getRank() != this.getRank()) {
//            throw new IllegalArgumentException("Ranks differ");
//        }
        validateRestoreArg(accessor);

        return new Quad(
                accessor.get(obj, 3),
                accessor.get(obj, 0),
                accessor.get(obj, 1),
                accessor.get(obj, 2));
    }
}
