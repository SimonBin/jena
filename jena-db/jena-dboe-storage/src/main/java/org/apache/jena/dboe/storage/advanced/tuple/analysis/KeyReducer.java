package org.apache.jena.dboe.storage.advanced.tuple.analysis;

/**
 * Accumulate a value from an object at a given index.
 * The object can be seen as the value of a component in a conceptual tuple.
 *
 * Note that the use of this interface is primarily for reducing keys in index nodes
 * The interpretation of keys - especially which tuple components can be extracted from it - depends on the index.
 *
 *
 * @author raven
 *
 * @param <K>
 */
public interface KeyReducer<K>
{
    public K reduce(K accumulator, int indexNode, Object value);
}
