package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNode;

public class StorageNodeAndStoreAndCodec<D2, C2>
    extends StorageNodeAndStore<D2, C2>
{
    protected TupleCodec<?, ?, D2, C2> tupleCodec;
    protected HyperTrieAccessor<C2> hyperTrieAccessor;

    public StorageNodeAndStoreAndCodec(
            StorageNode<D2, C2, ?> storage,
            Object store,
            TupleCodec<?, ?, D2, C2> tupleCodec,
            HyperTrieAccessor<C2> hyperTrieAccessor) {
        super(storage, store);
        this.tupleCodec = tupleCodec;
        this.hyperTrieAccessor = hyperTrieAccessor;
    }

    public TupleCodec<?, ?, D2, C2> getTupleCodec() {
        return tupleCodec;
    }

    public HyperTrieAccessor<C2> getHyperTrieAccessor() {
        return hyperTrieAccessor;
    }

}
