package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Set;

public interface HyperTrieAccessor<C> {
//    int getIndexedComponentIdx();

    Set<C> getValuesForComponent(Object altStore, int componentIdx);
    Object getStoreForSliceByComponentByValue(Object store, int componentIdx, C value);

    HyperTrieAccessor<C> getAccessorForComponent(int componentIdx);
}
