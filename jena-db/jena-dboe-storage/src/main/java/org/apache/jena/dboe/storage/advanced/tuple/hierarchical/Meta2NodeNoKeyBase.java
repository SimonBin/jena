package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.ext.com.google.common.collect.Maps;


/**
 * Base class for index nodes that do not index by a key - or rather:
 * index by a single key that is a zero-sized tuple
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public abstract class Meta2NodeNoKeyBase<D, C, V>
    extends Meta2NodeBase<D, C, V>
{
    public Meta2NodeNoKeyBase(TupleAccessor<D, C> tupleAccessor) {
        super(new int[] {}, tupleAccessor);
    }


    @Override
    public <T> Streamer<V, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        throw new UnsupportedOperationException("Cannot stream keys as components if there are no keys");
    }

    @Override
    public <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(TupleFactory.create0());
    }

    @Override
    public <T> Streamer<V, V> streamerForValues(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(argStore);
    }


    @Override
    public <T> Streamer<V, ?> streamerForKeys(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(TupleFactory.create0());
    }

    @Override
    public <T> Streamer<V, ? extends Entry<?, ?>> streamerForKeyAndSubStores(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), argStore));
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        throw new RuntimeException("Key is an empty tuple - there are no key components");
    }
}
