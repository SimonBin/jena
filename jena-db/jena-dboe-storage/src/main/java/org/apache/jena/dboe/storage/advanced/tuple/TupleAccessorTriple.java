package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TupleAccessorTriple
    implements TupleAccessor<Triple, Node>
{
    public static final TupleAccessorTriple INSTANCE = new TupleAccessorTriple();

    @Override
    public int getRank() {
        return 3;
    }

    @Override
    public Node get(Triple triple, int idx) {
        return getComponent(triple, idx);
    }

    /**
     * Surely there is some common util method somewhere?
     *
     *
     * @return
     */
    public static Node getComponent(Triple triple, int idx) {
        switch (idx) {
        case 0: return triple.getSubject();
        case 1: return triple.getPredicate();
        case 2: return triple.getObject();
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }

//    @Override
//    public Triple restore(Node[] components) {
//        validateRestoreArg(components);
//
//        return new Triple(components[0], components[1], components[2]);
//    }

    @Override
    public <T> Triple restore(T obj, TupleAccessor<? super T, ? extends Node> accessor) {
        validateRestoreArg(accessor);

        return new Triple(
                accessor.get(obj, 0),
                accessor.get(obj, 1),
                accessor.get(obj, 2));
    }

}
