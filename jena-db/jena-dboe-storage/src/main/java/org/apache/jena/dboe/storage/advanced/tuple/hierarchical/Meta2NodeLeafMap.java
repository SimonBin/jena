package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public class Meta2NodeLeafMap<D, C, K, V>
    extends Meta2NodeMapBase<D, C, K, V>
{
    protected TupleValueFunction<C, V> valueFunction;

    public Meta2NodeLeafMap(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier,
            TupleValueFunction<C, K> keyFunction,
            TupleAccessorCore<? super K, ? extends C> keyToComponent,
            TupleValueFunction<C, V> valueFunction
            ) {
        super(tupleIdxs, tupleAccessor, mapSupplier, keyFunction, keyToComponent);
        this.valueFunction = valueFunction;
    }

    @Override
    public List<Meta2Node<D, C, ?>> getChildren() {
        return Collections.emptyList();
    }

    // @Override
//    public K tupleToKey(D tupleLike, TupleAccessor<? super D, ? extends C> tupleAccessor) {
//        K result = keyFunction.createKey(tupleLike, tupleAccessor);
//        return result;
//    }

    @Override
    public boolean add(Map<K, V> map, D tupleLike) {
        K key = tupleToKey(tupleLike);
        V newValue = valueFunction.map(tupleLike, tupleAccessor);

        if(map.containsKey(key)) {
            V oldValue = map.get(key);
            throw new RuntimeException("Key " + key + " already mapped to " + oldValue);
        } else {
            map.put(key, newValue);
        }

        return true;
    }

    @Override
    public boolean remove(Map<K, V> map, D tupleLike) {
        K key = tupleToKey(tupleLike);
        boolean result = map.containsKey(key);
        if (result) {
            map.remove(key);
        }

        return result;
    }

    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + ")";
    }

    @Override
    public <T> Stream<Entry<K, ?>> streamEntries(Map<K, V> map, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        // Check whether the components of the given tuple are all non-null such that we can
        // create a key from them
        Object[] tmp = new Object[tupleIdxs.length];
        boolean eligibleAsKey = true;
        for (int i = 0; i < tupleIdxs.length; ++i) {
            C componentValue = tupleAccessor.get(tupleLike, i);
            if (componentValue == null) {
                eligibleAsKey = false;
                break;
            }
            tmp[i] = componentValue;
        }

        Stream<Entry<K, ?>> childStream;

        // If we have a key we can do a lookup in the map
        // otherwise we have to scan all keys
        if (eligibleAsKey) {
            K key = keyFunction.map(tmp, (x, i) -> (C)x[i]);

            V value = map.get(key);
            childStream = value == null
                    ? Stream.empty()
                    : Stream.of(new SimpleEntry<>(key, value));
        } else {
            childStream = map.entrySet().stream().map(e -> new SimpleEntry<>(e.getKey(), e.getValue()));
        }

        return childStream;
    }

}