package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNode;

public class StorageNodeAndStore<D, C> {
    protected StorageNode<D, C, ?> storage;
    protected Object store;

    public StorageNodeAndStore(StorageNode<D, C, ?> storage, Object store) {
        super();
        this.storage = storage;
        this.store = store;
    }

    public StorageNode<D, C, ?> getStorage() {
        return storage;
    }

    public Object getStore() {
        return store;
    }
}