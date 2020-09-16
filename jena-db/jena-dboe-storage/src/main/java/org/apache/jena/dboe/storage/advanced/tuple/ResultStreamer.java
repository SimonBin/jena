package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.stream.Stream;

public interface ResultStreamer<D, C, T> {
    /*
     * Domain implies tuple and tuple implies component. This is transitive, i.e. domain implies component.
     * Items with lower ordinal() imply capabilities for all items with higher one
     */
//    enum Capabilities {
//        DOMAIN,
//        TUPLE,
//        COMPONENT
//    }

    Stream<D> streamAsDomainObjects();
    Stream<C> streamAsComponent();
    Stream<T> streamAsTuple();

    /**
     * A set describing which methods are valid to invoke
     *
     *
     * @return
     */
//    EnumSet<Capabilities> getCapabilities();
//    getCapabilitiy();
}
