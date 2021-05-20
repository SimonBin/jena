package org.apache.jena.dboe.storage.advanced.tuple.tag;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;

/**
 * Interface for implementations providing hyper-trie data access
 *
 * @author raven
 *
 * @param <C> The component type of tuples indexed by the typer-trie
 */
public interface HasHyperTrieAccessor<C>
{
    HyperTrieAccessor<C> getHyperTrieAccessor();
}
