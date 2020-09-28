package org.apache.jena.dboe.storage.advanced.triple;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessors;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.HyperTrieBased;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleTableFromHyperTrie<V>
    extends TripleTableFromStorageNode<V>
    implements HyperTrieBased<Node>
{
    protected HyperTrieAccessor<Node> hyperTrieAccessor;

    public TripleTableFromHyperTrie(StorageNodeMutable<Triple, Node, V> rootStorageNode, V store) {
        super(rootStorageNode, store);
        this.hyperTrieAccessor = HyperTrieAccessors.index(rootStorageNode);
    }

    @Override
    public HyperTrieAccessor<Node> getHyperTrieAccessor() {
        return hyperTrieAccessor;
    }

}
