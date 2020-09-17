package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public interface StorageNodeBased<D, C, V> {
    StorageNodeMutable<D, C, V> getStorageNode();
    V getStore();
}
