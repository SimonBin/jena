package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.stream.Stream;

public interface StreamTransform<I, O> {
    Stream<O> transform(Stream<I> in);
}
