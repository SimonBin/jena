package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Collection;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 * FIXME The naming is horrible - it should be someting like StorageExpression or StorageFactory or
 * StorageManager.
 * The 'Meta' in the name at present is because it is not the store itself but the factory for it
 * 'Node' because it is a node in a tree (or an expression)
 * '2' because its the second design
 *
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public interface Meta2Node<D, C, V> {
    /**
     * Each node in the storage expression may have 0 or more children
     *
     * @return
     */
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


    <T> Stream<?> streamEntries(Object store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor);

    /**
     * Generic method to stream the content - mainly for debugging
     *
     * @return
     */
    default Stream<?> streamEntries(Object store) {
        // Stream with a null-tuple for which every component is reported as null (unconstrained)
        return streamEntries(store, null, (x, i) -> null);
    }
}
