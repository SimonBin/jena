package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public interface StorageNodeMutable<D, C, V>
    extends StorageNode<D, C, V>
{
    V newStore();

    boolean isEmpty(V store);
    boolean add(V store, D tupleLike);
    boolean remove(V store, D tupleLike);

    void clear(V store);

    @SuppressWarnings("unchecked")
    default boolean isEmptyRaw(Object store) {
        return isEmpty((V)store);
    }

    @SuppressWarnings("unchecked")
    default boolean addRaw(Object store, D tupleLike) {
        return add((V)store, tupleLike);
    }

    @SuppressWarnings("unchecked")
    default boolean removeRaw(Object store, D tupleLike) {
        return remove((V)store, tupleLike);
    }

    @SuppressWarnings("unchecked")
    default void clearRaw(Object store) {
        clear((V)store);
    }

}
