package org.apache.jena.dboe.storage.advanced.tuple.analysis;

/**
 * Reducer for key and value provided as separate arguments.
 * Similar to {@link IndexedKeyReducer} but this one allows for generic keys.
 *
 * @author raven
 *
 */
public interface BiReducer<A, K, V> {
    A reduce(A accumulator, K key, V value);
}
