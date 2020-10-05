package org.apache.jena.dboe.storage.advanced.triple;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessors;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.HyperTrieBased;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodecBased;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleTableFromHyperTrie<V>
    extends TripleTableFromStorageNode<V>
    implements HyperTrieBased<Node>, TupleCodecBased<Triple, Node, Triple, Node>
{
    protected HyperTrieAccessor<Node> hyperTrieAccessor;
    /** A codec that typically is either the identity mapping or a normalization */
    protected TupleCodec<Triple, Node, Triple, Node> tupleCodec;


    public TripleTableFromHyperTrie(StorageNodeMutable<Triple, Node, V> rootStorageNode, V store,
            HyperTrieAccessor<Node> hyperTrieAccessor, TupleCodec<Triple, Node, Triple, Node> tupleCodec) {
        super(rootStorageNode, store);
        this.hyperTrieAccessor = hyperTrieAccessor;
        this.tupleCodec = tupleCodec;
    }

    @Override
    public TupleCodec<Triple, Node, Triple, Node> getTupleCodec() {
        return tupleCodec;
    }

    @Override
    public HyperTrieAccessor<Node> getHyperTrieAccessor() {
        return hyperTrieAccessor;
    }

    public static <V> TripleTableFromHyperTrie<V> create(StorageNodeMutable<Triple, Node, V> rootStorageNode) {
        return create(rootStorageNode, null);
    }

    public static <V> TripleTableFromHyperTrie<V> create(
            StorageNodeMutable<Triple, Node, V> rootStorageNode,
            TupleCodec<Triple, Node, Triple, Node> tupleCodec) {
        V store = rootStorageNode.newStore();
        HyperTrieAccessor<Node> hyperTrieAccessor = HyperTrieAccessors.index(rootStorageNode);
        return new TripleTableFromHyperTrie<V>(rootStorageNode, store, hyperTrieAccessor, tupleCodec);
    }

}
