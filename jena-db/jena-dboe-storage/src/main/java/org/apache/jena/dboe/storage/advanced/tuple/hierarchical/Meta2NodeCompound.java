package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public interface Meta2NodeCompound<D, C, V>
    extends Meta2Node<D, C, V>
{
//    Meta2Node<D, C, V> getChild();

    V newStore();

    boolean isEmpty(Object store);
    boolean add(Object store, D tupleLike);
    boolean remove(Object store, D tupleLike);
}