package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;

public interface HyperTrieBased<C>
{
    HyperTrieAccessor<C> getHyperTrieAccessor();
}
