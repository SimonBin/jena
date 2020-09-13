package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

abstract class Meta2NodeMapBase<D, C, K, V>
    extends Meta2NodeBase<D, C, Map<K, V>>
    implements Meta2NodeCompound<D, C, Map<K, V>>
{
    protected MapSupplier mapSupplier;
    // protected TupleToKey<? extends K, C> keyFunction;
    TupleValueFunction<C, K> keyFunction;

    public K tupleToKey(D tupleLike) {
        K result = keyFunction.map(tupleLike, (d, i) -> tupleAccessor.get(d, tupleIdxs[i]));
        return result;
    }


    @SuppressWarnings("unchecked")
    public Map<K, V> asMap(Object store) {
        return (Map<K, V>)store;
    }


    public Meta2NodeMapBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier,
            //TupleToKey<? extends K, C> keyFunction
            //Function<? super D, ? extends K> keyFunction
            TupleValueFunction<C, K> keyFunction
            ) {
        super(tupleIdxs, tupleAccessor);
        this.mapSupplier = mapSupplier;
        this.keyFunction = keyFunction;
    }

    @Override
    public Map<K, V> newStore() {
        return mapSupplier.get();
    }

    @Override
    public boolean isEmpty(Object store) {
        Map<K, V> map = asMap(store);
        boolean result = map.isEmpty();
        return result;
    }
}