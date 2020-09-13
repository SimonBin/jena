package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public class Meta2NodeLeafSet<D, C, V>
    extends Meta2NodeSetBase<D, C, V>
{
    protected TupleValueFunction<C, V> valueFunction;

    public Meta2NodeLeafSet(
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
    public List<Meta2Node<D, C, ?>> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public boolean add(Object store, D tupleLike) {
        Set<V> set = asSet(store);
        V newValue = tupleToValue(tupleLike);
        boolean result = set.add(newValue);
        return result;
    }

    @Override
    public boolean remove(Object store, D tupleLike) {
        Set<V> set = asSet(store);
        V newValue = tupleToValue(tupleLike);
        boolean result = set.remove(newValue);
        return result;
    }

    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + ")";
    }

    @Override
    public <T> Stream<V> streamEntries(Object store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        Set<V> set = asSet(store);
        return set.stream();
    }

}