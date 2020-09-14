package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

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

    @Override
    public <T> Streamer<Map<K, V>, C> streamerForKeysAsComponent(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {

        Streamer<Map<K, V>, K> baseStreamer = streamerForKeysUnderConstraints(pattern, accessor);
        // FIXME Ensure that the keys can be cast as components!
        return argMap -> baseStreamer.stream(argMap).map(key -> (C)key);
    }


    @Override
    public <T> Streamer<Map<K, V>, Tuple<C>> streamerForKeysAsTuples(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        // TODO Auto-generated method stub
        return null;
    }

}