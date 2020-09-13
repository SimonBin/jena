package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

public abstract class TupleTableCoreFromSet<TupleType, ComponentType>
    implements TupleTableCore<TupleType, ComponentType>
{
    protected Set<TupleType> set;

    public TupleTableCoreFromSet() {
        this (new LinkedHashSet<>());
    }

    public TupleTableCoreFromSet(Set<TupleType> set) {
        super();
        this.set = set;
    }


    @Override
    public void clear() {
        set.clear();
    }

    @Override
    public long size() {
        return set.size();
    }

    @Override
    public void add(TupleType tuple) {
        set.add(tuple);
    }

    @Override
    public void delete(TupleType tuple) {
        set.remove(tuple);
    }

    @Override
    public boolean contains(TupleType tuple) {
        return set.contains(tuple);
    }

    @Override
    public Stream<TupleType> findTuples() {
        return set.stream();
    }
}
