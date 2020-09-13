package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.jena.dboe.storage.advanced.tuple.MapSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public class Meta2NodeInnerMap<D, C, K, V>
    extends Meta2NodeMapBase<D, C, K, V>
{
    protected Meta2NodeCompound<D, C, V> child;

    public Meta2NodeInnerMap(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            Meta2NodeCompound<D, C, V> child,
            MapSupplier mapSupplier,
            Function<? super D, ? extends K> keyFunction) {
        super(tupleIdxs, tupleAccessor, mapSupplier, keyFunction);
        this.child = child;
    }


    @Override
    public Collection<Meta2Node<D, C, ?>> getChildren() {
        return Collections.singleton(child);
    }

    @Override
    public Map<K, V> newStore() {
        return mapSupplier.newMap();
    }

    // @Override
    public K tupleToKey(D tupleLike) {
        K result = keyFunction.apply(tupleLike);
        return result;
    }

    @Override
    public boolean add(Object store, D tupleLike) {
        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>)store;

        K key = keyFunction.apply(tupleLike);

        V v = map.get(key);
        if (v == null) {
            // TODO If we need to create a new child store then the result of this function
            // must be true - validate that child.add also returns true
            v = child.newStore();
            map.put(key, v);
        }

        boolean result = child.add(v, tupleLike);
        return result;
    }

    @Override
    public boolean remove(Object store, D tupleLike) {
        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>)store;

        K key = keyFunction.apply(tupleLike);

        boolean result = false;
        V v = map.get(key);
        if (v != null) {
            result = child.remove(v, tupleLike);
            if (child.isEmpty(store)) {
                map.remove(key);
            }
        }

        return result;
    }


    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + " -> " + Objects.toString(child) + ")";
    }
}

