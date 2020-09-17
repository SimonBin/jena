package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.stream.Stream;


/**
 * A streamer returns a stream of items from a collection-like object given
 * as the argument. For example, in the case of a map data structure a
 * streamer may return any of its key, value or entry set. If the type actual
 * type of the collection is not known the {{@link #streamRaw(Object)} method
 * should be used.
 *
 * @author raven
 *
 * @param <V>
 * @param <T>
 */
@FunctionalInterface
public interface Streamer<V, T> {

    Stream<T> stream(V store);

    @SuppressWarnings("unchecked")
    default Stream<T> streamRaw(Object store) {
        return stream((V)store);
    }
}
