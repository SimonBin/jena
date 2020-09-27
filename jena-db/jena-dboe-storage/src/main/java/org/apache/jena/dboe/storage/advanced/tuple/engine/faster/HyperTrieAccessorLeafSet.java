package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeLeafSet;

public class HyperTrieAccessorLeafSet<C>
    implements HyperTrieAccessor<C>
{
    protected StorageNodeLeafSet<?, C, C> storageNode;

    public HyperTrieAccessorLeafSet(StorageNodeLeafSet<?, C, C> storageNode) {
        super();
        this.storageNode = storageNode;
    }

    @Override
    public Set<C> getValuesForComponent(Object altStore, int componentIdx) {
        assert storageNode.getKeyTupleIdxs()[0] == componentIdx;

        return (Set<C>)altStore;
    }

    @Override
    public Object getStoreForSliceByComponentByValue(Object altStore, int componentIdx, C value) {
        assert storageNode.getKeyTupleIdxs()[0] == componentIdx;

        Set<C> set = (Set<C>)altStore;
        Object result = set.contains(value) ? this : null;
        return result;
    }

    @Override
    public HyperTrieAccessor<C> getAccessorForComponent(int componentIdx) {
        throw new UnsupportedOperationException("A leaf node doesn't have further components that can be accessed");
//        assert storageNode.getKeyTupleIdxs()[0] == componentIdx;
//
//        return this;
    }

}
