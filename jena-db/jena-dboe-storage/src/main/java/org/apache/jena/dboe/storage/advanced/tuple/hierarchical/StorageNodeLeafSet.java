package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.ext.com.google.common.collect.Maps;


public class StorageNodeLeafSet<D, C, V>
    extends StorageNodeSetBase<D, C, V>
{
    protected TupleValueFunction<C, V> valueFunction;

    public StorageNodeLeafSet(
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier,
            TupleValueFunction<C, V> valueFunction
            ) {
        super(new int[] {}, tupleAccessor, setSupplier);
        this.valueFunction = valueFunction;
    }

    public V tupleToValue(D tupleLike) {
        V result = valueFunction.map(tupleLike, (d, i) -> tupleAccessor.get(d, tupleIdxs[i]));
        return result;
    }


    @Override
    public List<StorageNode<D, C, ?>> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public boolean add(Set<V> set, D tupleLike) {
        V newValue = tupleToValue(tupleLike);
        boolean result = set.add(newValue);
        return result;
    }

    @Override
    public boolean remove(Set<V> set, D tupleLike) {
        V newValue = tupleToValue(tupleLike);
        boolean result = set.remove(newValue);
        return result;
    }

    @Override
    public void clear(Set<V> store) {
        store.clear();
    }

    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + ")";
    }

    @Override
    public <T> Streamer<Set<V>, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        throw new UnsupportedOperationException("Cannot stream keys as components if there are no keys");
    }

    @Override
    public <T> Streamer<Set<V>, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(TupleFactory.create0());
    }

    @Override
    public <T> Streamer<Set<V>, V> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return argSet -> argSet.stream();
    }

//    @Override
//    public Streamer<Set<V>, V> streamerForValues() {
//        return argSet -> argSet.stream();
//    }


    @Override
    public <T> Streamer<Set<V>, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(
//            int altIdx,
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argSet -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), argSet));
//        throw new UnsupportedOperationException("leaf sets do not have a sub store");
//    	return argSet -> argSet.stream().map(item -> Entry<>);
    }

    @Override
    public <T> Stream<V> streamEntries(Set<V> set, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        // FIXME We need to filter the result stream by the components of the tuple like!
        return set.stream();
    }

    @Override
    public <T> Streamer<Set<V>, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        throw new RuntimeException("Key is an empty tuple - there are no key components");
    }

//    @Override
//    public Object chooseSubStore(Set<V> store, int subStoreIdx) {
//        throw new UnsupportedOperationException("leaf sets do not have a sub store");
//    }

    @Override
    public Object chooseSubStore(Set<V> store, int subStoreIdx) {
        if (subStoreIdx != 0) {
            throw new IndexOutOfBoundsException("Index must be 0 for inner maps");
        }

        // Return the store itself
        return store;
    }

}