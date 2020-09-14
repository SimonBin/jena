package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.ext.com.google.common.collect.Maps;

abstract class Meta2NodeMapBase<D, C, K, V>
    extends Meta2NodeBase<D, C, Map<K, V>>
    implements Meta2NodeCompound<D, C, Map<K, V>>
{
    protected MapSupplier mapSupplier;
    // protected TupleToKey<? extends K, C> keyFunction;
    TupleValueFunction<C, K> keyFunction;

    // Reverse mapping of key to components
    TupleAccessorCore<? super K, ? extends C> keyToComponent;

    public K tupleToKey(D tupleLike) {
        K result = keyFunction.map(tupleLike, (d, i) -> tupleAccessor.get(d, tupleIdxs[i]));
        return result;
    }

//
//    @SuppressWarnings("unchecked")
//    public Map<K, V> asMap(Object store) {
//        return (Map<K, V>)store;
//    }


    public Meta2NodeMapBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier,
            //TupleToKey<? extends K, C> keyFunction
            //Function<? super D, ? extends K> keyFunction
            TupleValueFunction<C, K> keyFunction,
            TupleAccessorCore<? super K, ? extends C> keyToComponent
            ) {
        super(tupleIdxs, tupleAccessor);
        this.mapSupplier = mapSupplier;
        this.keyFunction = keyFunction;
        this.keyToComponent = keyToComponent;
    }

    @Override
    public Map<K, V> newStore() {
        return mapSupplier.get();
    }

    @Override
    public boolean isEmpty(Map<K, V> map) {
        boolean result = map.isEmpty();
        return result;
    }


    public <T> Streamer<Map<K, V>, K> streamerForKeysUnderConstraints(
            T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor)
    {
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

        Streamer<Map<K, V>, K> result;

        if (eligibleAsKey) {
            K key = keyFunction.map(tmp, (x, i) -> (C)x[i]);

            result = argMap -> argMap.containsKey(key)
                    ? Stream.of(key)
                    : Stream.empty();
        } else {
            result = argMap -> argMap.keySet().stream();
        }

        return result;
    }

    public <T> Streamer<Map<K, V>, Entry<K, V>> streamerForEntriesUnderConstraints(
            T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor)
    {
        Object[] tmp = new Object[tupleIdxs.length];
        boolean eligibleAsKey = true;
        for (int i = 0; i < tupleIdxs.length; ++i) {
            C componentValue = tupleAccessor.get(tupleLike, tupleIdxs[i]);
            if (componentValue == null) {
                eligibleAsKey = false;
                break;
            }
            tmp[i] = componentValue;
        }

        Streamer<Map<K, V>, Entry<K, V>> result;

        if (eligibleAsKey) {
            K key = keyFunction.map(tmp, (x, i) -> (C)x[i]);

            result = argMap -> argMap.containsKey(key)
                    ? Stream.of(Maps.immutableEntry(key, argMap.get(key)))
                    : Stream.empty();
        } else {
            result = argMap -> argMap.entrySet().stream();
        }

        return result;
    }


    @Override
    public <T> Streamer<Map<K, V>, C> streamerForKeysAsComponent(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {

        Streamer<Map<K, V>, K> baseStreamer = streamerForKeysUnderConstraints(pattern, accessor);
        // FIXME Ensure that the keys can be cast as components!
        return argMap -> baseStreamer.stream(argMap).map(key -> (C)key);
    }


    @Override
    public <T> Streamer<Map<K, V>, V> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        Streamer<Map<K, V>, K> baseStreamer = streamerForKeysUnderConstraints(pattern, accessor);

        // The base streamer ensures that the key exists in the map
        return argMap -> baseStreamer.stream(argMap).map(key -> argMap.get(key));
    }

    @Override
    public <T> Streamer<Map<K, V>, Tuple<C>> streamerForKeysAsTuples(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        // FIXME Implement Auto-generated method stub
        return null;
    }

    @Override
    public <T> Streamer<Map<K, V>, K> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return streamerForKeysUnderConstraints(pattern, accessor);
    }


    @Override
    public <T> Streamer<Map<K, V>, Entry<K, V>> streamerForKeyAndSubStores(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        // TODO Assert that altIdx == 0
        return streamerForEntriesUnderConstraints(pattern, accessor);
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        C result = keyToComponent.get((K)key, idx);
        return result;
    }

    public C getKeyComponent(K key, int idx) {
        C result = keyToComponent.get(key, idx);
        return result;
    }
}