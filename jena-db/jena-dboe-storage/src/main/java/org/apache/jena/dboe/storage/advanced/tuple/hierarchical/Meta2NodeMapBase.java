package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map;
import java.util.function.Function;

import org.apache.jena.dboe.storage.advanced.tuple.MapSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

abstract class Meta2NodeMapBase<D, C, K, V>
    extends Meta2NodeBase<D, C, Map<K, V>>
    implements Meta2NodeCompound<D, C, Map<K, V>>
{
    protected MapSupplier mapSupplier;
    // protected TupleToKey<? extends K, C> keyFunction;
    Function<? super D, ? extends K> keyFunction;

    public Meta2NodeMapBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier,
            //TupleToKey<? extends K, C> keyFunction
            Function<? super D, ? extends K> keyFunction
            ) {
        super(tupleIdxs, tupleAccessor);
        this.mapSupplier = mapSupplier;
        this.keyFunction = keyFunction;
    }

    @Override
    public Map<K, V> newStore() {
        return mapSupplier.newMap();
    }

    @Override
    public boolean isEmpty(Object store) {
        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>)store;

        boolean result = map.isEmpty();
        return result;
    }
}