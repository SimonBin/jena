package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;

public class StorageNodeAndStoreAndCodec<D2, C2>
    extends StorageNodeAndStore<D2, C2>
{
    protected TupleCodec<?, ?, D2, C2> tupleCodec;

    public StorageNodeAndStoreAndCodec(StorageNode<D2, C2, ?> storage, Object store,  TupleCodec<?, ?, D2, C2> tupleCodec) {
        super(storage, store);
        this.tupleCodec = tupleCodec;
    }

    public TupleCodec<?, ?, D2, C2> getTupleCodec() {
        return tupleCodec;
    }

}
