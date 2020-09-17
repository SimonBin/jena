package org.apache.jena.dboe.storage.advanced.quad;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableFromStorageNodeBase;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Adaption of a tuple table to the domain of quads
 *
 * @author raven
 *
 * @param <V>
 */
public class QuadTableFromStorageNode<V>
    extends TupleTableFromStorageNodeBase<Quad, Node, V>
    implements QuadTableCore
{
    public QuadTableFromStorageNode(
            StorageNodeMutable<Quad, Node, V> rootStorageNode,
            V store) {
        super(rootStorageNode, store);
    }

    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        return newFinder().eq(0, s).eq(1, p).eq(2, o).eq(3, g).stream();
    }

    @Override
    public Stream<Node> listGraphNodes() {
        return newFinder().projectOnly(3).distinct().stream();
    }


    public static <V> QuadTableFromStorageNode<V> create(StorageNodeMutable<Quad, Node, V> rootStorageNode) {
        V store = rootStorageNode.newStore();
        return new QuadTableFromStorageNode<V>(rootStorageNode, store);
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
