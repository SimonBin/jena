package org.apache.jena.dboe.storage.advanced.tuple.analysis;

public interface KeyReducer2<K> {
    K reduce(K accumulator, Object contribution);


    static <K> K passOn(K accumulator, Object contribution) {
        return accumulator;
    }

}
