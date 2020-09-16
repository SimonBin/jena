package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 * A StorageNode can express nested storage such as nested maps, lists and sets for tuples.
 * A Storage may have alternative child structures to choose from. All alternatives are assumed
 * to be in sync and contain the same instance data - but the nesting may differ.
 *
 *
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public interface StorageNode<D, C, V> {

    /**
     * Each node in the storage expression may have 0 or more children
     *
     * @return
     */
    List<? extends StorageNode<D, C, ?>> getChildren();

    /**
     * Future work; return a histogram for a corresponding store
     * Perhaps this method fits better on a derived interface such as IndexNodeMap
     *
     */
    // Histogram getHistogram(V store)

    /**
     * Should a node have a parent?
     * If we wanted to use subtrees of index nodes in different settings - especially
     * placing a bunch of them into a alternatives - then the parent differs from the context
     * Therefore the parent is left out
     *
     *
     */
//    Meta2Node<D, C, ?> getParent();


    /**
     * The component indexes by which this node indexes
     * May be empty but never null
     *
     * @return
     */
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


    /**
     * Compiles from a given pattern a function that can stream the matching
     * keys from the appropriate store.
     * The keys must be instances of the component type otherwise an exception is raised
     *
     *
     * @return
     */
    <T> Streamer<V, C> streamerForKeysAsComponent(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    /**
     *
     * Compiles from a given pattern a function that can stream the matching
     * keys from the appropriate store.
     * The keys must be instances of the component type otherwise an exception is raised
     *
     * if getKeyTupleIdxs().length == 0 then returns a single tuple that projects no components
     *
     * @param store
     * @return
     */
    <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    <T> Streamer<V, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    C getKeyComponentRaw(Object key, int idx);

    Object chooseSubStore(V store, int subStoreIdx);

    @SuppressWarnings("unchecked")
    default Object chooseSubStoreRaw(Object store, int subStoreIdx) {
        Object result;
        try {
            result = chooseSubStore((V)store, subStoreIdx);
        } catch (Exception e) {
            throw new RuntimeException("Mismatch of data and schema;"
                    + "could not access alt " + subStoreIdx
                    + " at a schema of type " + this.getClass()
                    + " for data of type " +  store.getClass()
                    + " " + store, e);
        }
        return result;
    }


    /**
     * Returns an object that can extract the stream of values at an index node under constraints
     *
     * map: { entry.value | entry in map and entry.key matches condition}
     * leafSet: set.entries.stream() A leaf set does not have any keys that could affect the result stream
     *
     * @param <T>
     * @param pattern
     * @param accessor
     * @return
     */
    <T> Streamer<V, ?> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);

    /**
     * The streamer returns entry that hold a tuple-like key and conceptually alternatives of sub-stores
     *
     * The tuple components of the key can be accessed using {@link #getKeyComponentRaw(Object, int)}
     * There are as many components as the length of {@link #getKeyTupleIdxs()}
     * If there is 0 components then any invocation of {@link #getKeyComponentRaw(Object, int)} will fail with a
     * {@link UnsupportedOperationException}.
     *
     * To extract a specific alternative from the substore use {@link #chooseSubStore(Object, int)}.
     * If {@link #getChildren()} is not empty then there are as many substore indices as there are children
     * If there are no children then there is an implicit single sub store with index 0
     *
     *
     * @param <T>
     * @param pattern
     * @param accessor
     * @return
     */
    <T> Streamer<V, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(
            T pattern, TupleAccessorCore<? super T, ? extends C> accessor);



    /*
     * Methods for immediately iterating the content - useful for debugging but not intended
     * for building upon
     *
     * Use the streamerFor* methods for production code
     *
     */

    /**
     * Stream all entries under equality constraints obtained from a tuple-like pattern
     *
     * @param <T>
     * @param store
     * @param tupleLike
     * @param tupleAccessor
     * @return
     */
    <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor);

    @SuppressWarnings("unchecked")
    default <T> Stream<?> streamEntriesRaw(Object store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return streamEntries((V)store, tupleLike, tupleAccessor);
    }

    /**
     * Generic method to stream the content - mainly for debugging
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    default Stream<?> streamEntriesRaw(Object store) {
        // Stream with a null-tuple for which every component is reported as null (unconstrained)
        return streamEntries((V)store);
    }

    default Stream<?> streamEntries(V store) {
        // Stream with a null-tuple for which every component is reported as null (unconstrained)
        return streamEntriesRaw(store, null, (x, i) -> null);
    }

}
