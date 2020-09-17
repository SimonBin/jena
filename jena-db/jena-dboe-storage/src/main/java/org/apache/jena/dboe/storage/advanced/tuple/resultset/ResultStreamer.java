package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import java.util.stream.Stream;

/**
 * A class that acts as a factory for producing streams for the different aspects of tuples.
 * Specializations should convert between domain, tuple and component view whenever applicable.
 *
 * Examples: A component can be converted to a 1-tuple and vice versa.
 * A quad can be converted to a four tuple and vice versa.
 *
 * @author raven
 *
 * @param <D> The domain type such as Quad
 * @param <C> The component type such as Node
 * @param <T> The tuple type such as Tuple
 */
public interface ResultStreamer<D, C, T> {

    Stream<D> streamAsDomainObject();
    Stream<C> streamAsComponent();
    Stream<T> streamAsTuple();


    /*
     * Domain implies tuple and tuple implies component. This is transitive, i.e. domain implies component.
     * Items with lower ordinal() imply capabilities for all items with higher one
     */
//    enum Capabilities {
//        DOMAIN,
//        TUPLE,
//        COMPONENT
//    }

    /**
     * A set describing which methods are valid to invoke
     *
     *
     * @return
     */
//    EnumSet<Capabilities> getCapabilities();
//    getCapabilitiy();
}
