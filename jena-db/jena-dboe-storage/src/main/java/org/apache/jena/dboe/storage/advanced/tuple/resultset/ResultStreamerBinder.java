package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeCompound;

/**
 * Helper interface for use as a return type and in lambdas
 * for creating a
 * {@link ResultStreamer} instance from typically a store object
 * obtained via {@link StorageNodeCompound#newStore()}
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
