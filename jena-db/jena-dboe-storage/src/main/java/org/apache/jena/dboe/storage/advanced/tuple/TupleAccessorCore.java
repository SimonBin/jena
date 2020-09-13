package org.apache.jena.dboe.storage.advanced.tuple;

public interface TupleAccessorCore<TupleLike, ComponentType>
{
    ComponentType get(TupleLike tupleLike, int componentIdx);
}
