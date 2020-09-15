package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public class Meta2NodeInnerMap<D, C, K, V>
    extends Meta2NodeMapBase<D, C, K, V>
{
    protected Meta2NodeCompound<D, C, V> child;

    public Meta2NodeInnerMap(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            Meta2NodeCompound<D, C, V> child,
            MapSupplier mapSupplier,
            TupleValueFunction<C, K> keyFunction,
            TupleAccessorCore<? super K, ? extends C> keyToComponent) {
        super(tupleIdxs, tupleAccessor, mapSupplier, keyFunction, keyToComponent);
        this.child = child;
    }


    @Override
    public List<StorageNode<D, C, ?>> getChildren() {
        return Collections.singletonList(child);
    }

    @Override
    public Map<K, V> newStore() {
        return mapSupplier.get();
    }

    // @Override

    @Override
    public boolean add(Map<K, V> map, D tupleLike) {
        K key = tupleToKey(tupleLike);

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
    public boolean remove(Map<K, V> map, D tupleLike) {
        K key = tupleToKey(tupleLike);

        boolean result = false;
        V v = map.get(key);
        if (v != null) {
            result = child.remove(v, tupleLike);
            if (child.isEmpty(v)) {
                map.remove(key);
            }
        }

        return result;
    }


    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + " -> " + Objects.toString(child) + ")";
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

            V childStore = map.get(key);
            childStream = childStore == null
                    ? Stream.empty()
                    : child.streamEntries(childStore, tupleLike, tupleAccessor).map(v -> new SimpleEntry<>(key, v));
        } else {
            childStream = map.entrySet().stream().flatMap(
                    e -> child.streamEntries(e.getValue(), tupleLike, tupleAccessor).map(v -> new SimpleEntry<>(e.getKey(), v)));
        }

        return childStream;
    }


    @Override
    public Object chooseSubStore(Map<K, V> store, int subStoreIdx) {
        if (subStoreIdx != 0) {
            throw new IndexOutOfBoundsException("Index must be 0 for inner maps");
        }

        // Return the store itself
        return store;
    }
}

