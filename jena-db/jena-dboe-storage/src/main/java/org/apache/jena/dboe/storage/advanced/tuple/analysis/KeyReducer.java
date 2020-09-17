package org.apache.jena.dboe.storage.advanced.tuple.analysis;

public interface KeyReducer<K> {
    K reduce(K accumulator, Object contribution);


    /** KeyReducer that returns the accumulator directly and thus
     *  ignore the contribution.
     */
    static <K> K passThrough(K accumulator, Object contribution) {
        return accumulator;
    }
}
