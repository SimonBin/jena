package org.apache.jena.dboe.storage.advanced.tuple;

public interface TupleSetter<TupleType, ComponentType> {
    void set(TupleType tuple, int idx, ComponentType componentValue);
}
