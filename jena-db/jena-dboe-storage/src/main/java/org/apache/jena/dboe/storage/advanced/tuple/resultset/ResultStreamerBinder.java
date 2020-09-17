package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;

/**
 * Helper interface for use as a return type and in lambdas
 * for creating a
 * {@link ResultStreamer} instance from typically a store object
 * obtained via {@link StorageNodeMutable#newStore()}
 *
 * @author raven
 *
 * @param <D> The domain tuple type
 * @param <C> The component type
 * @param <T> The tuple type such as {@link Tuple}
 */
public interface ResultStreamerBinder<D, C, T>
{
    ResultStreamer<D, C, T> bind(Object store);
}
