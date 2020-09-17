package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public abstract class StorageNodeMutableForwarding<D, C, V, X extends StorageNodeMutable<D, C, V>>
    extends StorageNodeForwarding<D, C, V, X>
    implements StorageNodeMutable<D, C, V>
{
    @Override
    public V newStore() {
        return getDelegate().newStore();
    }

    @Override
    public boolean isEmpty(V store) {
        return getDelegate().isEmpty(store);
    }

    @Override
    public boolean add(V store, D tupleLike) {
        return getDelegate().add(store, tupleLike);
    }

    @Override
    public boolean remove(V store, D tupleLike) {
        return getDelegate().remove(store, tupleLike);
    }

    @Override
    public void clear(V store) {
        getDelegate().clear(store);
    }

}
