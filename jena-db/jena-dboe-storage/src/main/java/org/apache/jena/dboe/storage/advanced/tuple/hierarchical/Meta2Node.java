package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleSetter;

public interface Meta2Node<D, C, V> {
    Collection<Meta2Node<D, C, ?>> getChildren();
    int[] getKeyTupleIdxs();

    TupleAccessor<D, C> getTupleAccessor();

    /**
     * Create an object that can stream the content of the store
     *
     * @param <T>
     * @param tupleSupp
     * @param setter
     * @return
     */
//    <T> Stream<T> createStreamer(Supplier<T> tupleSupp, TupleSetter<T, C> setter);

}
