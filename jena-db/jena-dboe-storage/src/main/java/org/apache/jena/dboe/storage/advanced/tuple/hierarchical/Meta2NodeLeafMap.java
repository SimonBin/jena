package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.jena.dboe.storage.advanced.tuple.MapSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public class Meta2NodeLeafMap<D, C, K, V>
    extends Meta2NodeMapBase<D, C, K, V>
{
    protected Function<? super D, ? extends V> valueFunction;

    public Meta2NodeLeafMap(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier,
            Function<? super D, ? extends K> keyFunction,
            Function<? super D, ? extends V> valueFunction
            //TupleToKey<? extends K, C> keyFunction,
            //TupleToKey<? extends V, C> valueFunction
            ) {
        super(tupleIdxs, tupleAccessor, mapSupplier, keyFunction);
        this.valueFunction = valueFunction;
    }

    @Override
    public Collection<Meta2Node<D, C, ?>> getChildren() {
        return Collections.emptySet();
    }

    // @Override
//    public K tupleToKey(D tupleLike, TupleAccessor<? super D, ? extends C> tupleAccessor) {
//        K result = keyFunction.createKey(tupleLike, tupleAccessor);
//        return result;
//    }

    @Override
    public boolean add(Object store, D tupleLike) {
        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>)store;

        K key = keyFunction.apply(tupleLike);
        V newValue = valueFunction.apply(tupleLike);

        if(map.containsKey(key)) {
            V oldValue = map.get(key);
            throw new RuntimeException("Key " + key + " already mapped to " + oldValue);
        } else {
            map.put(key, newValue);
        }

        return true;
    }

    @Override
    public boolean remove(Object store, D tupleLike) {
        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>)store;

        K key = keyFunction.apply(tupleLike);
        boolean result = map.containsKey(key);
        if (result) {
            map.remove(result);
        }

        return result;
    }

    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + ")";
    }

}