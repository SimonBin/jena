package org.apache.jena.dboe.storage.advanced.triple;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableFromStorageNodeBase;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Adaption of a tuple table to the domain of quads
 *
 * @author raven
 *
 * @param <V>
 */
public class TripleTableFromStorageNode<V>
    extends TupleTableFromStorageNodeBase<Triple, Node, V>
    implements TripleTableCore
{
    public TripleTableFromStorageNode(
            StorageNodeMutable<Triple, Node, V> rootStorageNode,
            V store) {
        super(rootStorageNode, store);
    }

    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
        return newFinder().eq(0, s).eq(1, p).eq(2, o).stream();
    }

    // TODO We need to be wary of nulls / any!!!

//    @Override
//    public ResultStreamer<Quad, Node, Tuple<Node>> find(TupleQuery<Node> tupleQuery) {
//        NodeStats<Quad, Node> bestMatch = TupleQueryAnalyzer.analyze(tupleQuery, storeAccessor);
//        ResultStreamerBinder<Quad, Node, Tuple<Node>> binder = TupleQueryAnalyzer.createResultStreamer(
//                bestMatch,
//                tupleQuery,
//                TupleAccessorQuadAnyToNull.INSTANCE);
//
//        return binder.bind(store);
//    }

}
