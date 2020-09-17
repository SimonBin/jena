package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

abstract class StorageNodeSetBase<D, C, V>
    extends StorageNodeBase<D, C, Set<V>>
    implements StorageNodeMutable<D, C, Set<V>>
{
    protected SetSupplier setSupplier;

    public StorageNodeSetBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier
        ) {
        super(tupleIdxs, tupleAccessor);
        this.setSupplier = setSupplier;
    }

    @Override
    public Set<V> newStore() {
        return setSupplier.get();
    }

    @Override
    public boolean isEmpty(Set<V> store) {
        boolean result = store.isEmpty();
        return result;
    }
}