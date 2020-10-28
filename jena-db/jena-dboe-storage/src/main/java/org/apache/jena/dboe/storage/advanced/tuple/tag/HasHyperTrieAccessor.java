package org.apache.jena.dboe.storage.advanced.tuple.tag;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;

public interface HasHyperTrieAccessor<C>
{
    HyperTrieAccessor<C> getHyperTrieAccessor();
}
