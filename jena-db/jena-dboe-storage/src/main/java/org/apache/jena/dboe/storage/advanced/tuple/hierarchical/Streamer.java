package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.stream.Stream;

@FunctionalInterface
public interface Streamer<V, T> {

    Stream<T> stream(V store);

    @SuppressWarnings("unchecked")
    default Stream<T> streamRaw(Object store) {
        return stream((V)store);
    }
}
