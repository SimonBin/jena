package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

abstract class Meta2NodeSetBase<D, C, V>
    extends Meta2NodeBase<D, C, Set<V>>
    implements Meta2NodeCompound<D, C, Set<V>>
{
    protected SetSupplier setSupplier;

    public Meta2NodeSetBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier
        ) {
        super(tupleIdxs, tupleAccessor);
        this.setSupplier = setSupplier;
    }

    @SuppressWarnings("unchecked")
    public Set<V> asSet(Object store) {
        return (Set<V>)store;
    }

    @Override
    public Set<V> newStore() {
        return setSupplier.get();
    }

    @Override
    public boolean isEmpty(Object store) {
        Set<V> set = asSet(store);
        boolean result = set.isEmpty();
        return result;
    }
}