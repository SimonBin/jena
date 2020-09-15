package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.function.BiFunction;

public interface KeyReducerFactory<K> {
    BiFunction<K, Object, K> createForIndex(int i);
}
