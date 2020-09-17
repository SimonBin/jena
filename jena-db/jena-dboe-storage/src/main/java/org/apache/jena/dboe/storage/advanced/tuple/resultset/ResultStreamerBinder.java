package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2NodeCompound;

/**
 * Helper interface for use as a return type and in lambdas
 * for creating a
 * {@link ResultStreamer} instance from typically a store object
 * obtained via {@link Meta2NodeCompound#newStore()}
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <T>
 */
public interface ResultStreamerBinder<D, C, T>
{
    ResultStreamer<D, C, T> bind(Object store);
}
