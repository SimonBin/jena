package org.apache.jena.dboe.storage.advanced.tuple;

/**
 * Functional interface for accessing components of a tuple
 *
 * @author raven
 *
 * @param <TupleLike>
 * @param <ComponentType>
 */
@FunctionalInterface
public interface TupleAccessorCore<TupleLike, ComponentType>
{
    ComponentType get(TupleLike tupleLike, int componentIdx);
}
