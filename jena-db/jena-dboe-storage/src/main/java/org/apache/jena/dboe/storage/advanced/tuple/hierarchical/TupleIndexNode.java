package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.stream.Stream;

public interface TupleIndexNode<ThisKeyType, ThisValueType, NextValueType, IncomingValueType> {

    TupleIndexNode<?, ?, IncomingValueType, ?> getParent();

    // TupleIndexNode<?, ?, IncomingValueType, ?> getRoot();

//    List<?> getChildren();



//    void add(DomainType tuple);
//    void delete(DomainType tuple);

    // Stream<Map<K, V>> in, K k //, Predicate<? super K> isAny
    Stream<NextValueType> streamContributions(IncomingValueType incoming, ThisKeyType constraint);

    Stream<NextValueType> streamParent();
}
