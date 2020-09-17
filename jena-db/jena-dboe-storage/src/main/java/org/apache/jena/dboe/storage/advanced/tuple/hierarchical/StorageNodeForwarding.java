package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public abstract class StorageNodeForwarding<D, C, V extends StorageNode<D, C, V>>
    implements StorageNode<D, C, V>
{
    abstract V getDelegate();

    @Override
    public List<? extends StorageNode<D, C, ?>> getChildren() {
        return getDelegate().getChildren();
    }

    @Override
    public int[] getKeyTupleIdxs() {
        return getDelegate().getKeyTupleIdxs();
    }

    @Override
    public TupleAccessor<D, C> getTupleAccessor() {
        return getDelegate().getTupleAccessor();
    }

    @Override
    public <T> Streamer<V, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeysAsComponent(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeysAsTuples(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeys(pattern, accessor);
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        return getDelegate().getKeyComponentRaw(key, idx);
    }

    @Override
    public Object chooseSubStore(V store, int subStoreIdx) {
        return getDelegate().chooseSubStore(store, subStoreIdx);
    }

    @Override
    public <T> Streamer<V, ?> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForValues(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeyAndSubStoreAlts(pattern, accessor);
    }

    @Override
    public <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return getDelegate().streamEntries(store, tupleLike, tupleAccessor);
    }

}
