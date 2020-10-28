package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeLeafComponentSet;

public class HyperTrieAccessorLeafSet<C>
    implements HyperTrieAccessor<C>
{
    protected StorageNodeLeafComponentSet<?, C, ?> storageNode;
    protected int indexedComponentIdx;

    public HyperTrieAccessorLeafSet(StorageNodeLeafComponentSet<?, C, ?> storageNode) {
        super();
        this.storageNode = storageNode;
        if (storageNode.getKeyTupleIdxs().length != 1) {
            throw new IllegalArgumentException("Storage node must index by exactly 1 component");
        }

        this.indexedComponentIdx = storageNode.getKeyTupleIdxs()[0];
    }

//    @Override
//    public int getIndexedComponentIdx() {
//        return indexedComponentIdx;
//    }

    @Override
    public Set<C> getValuesForComponent(Object altStore, int componentIdx) {
        assert indexedComponentIdx == componentIdx;

        return (Set<C>)altStore;
    }

    @Override
    public Object getStoreForSliceByComponentByValue(Object altStore, int componentIdx, C value) {
        assert indexedComponentIdx == componentIdx;

        Set<C> set = (Set<C>)altStore;
        Object result = set.contains(value) ? this : null;
        return result;
    }

    @Override
    public HyperTrieAccessor<C> getAccessorForComponent(int componentIdx) {
        assert indexedComponentIdx == componentIdx;

        return this;
    }

}
