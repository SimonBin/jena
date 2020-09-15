package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public abstract class Meta2NodeBase<D, C, V>
    implements StorageNode<D, C, V>
{
    protected int[] tupleIdxs;
    protected TupleAccessor<D, C> tupleAccessor;

    public Meta2NodeBase(int[] tupleIdxs, TupleAccessor<D, C> tupleAccessor) {
        super();
        this.tupleIdxs = tupleIdxs;
        this.tupleAccessor = tupleAccessor;
    }

    @Override
    public int[] getKeyTupleIdxs() {
        return tupleIdxs;
    }

    @Override
    public TupleAccessor<D, C> getTupleAccessor() {
        return tupleAccessor;
    }
}