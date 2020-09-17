package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public abstract class StorageNodeCompoundBase<D, C, V>
    extends StorageNodeBase<D, C, V>
    implements StorageNodeMutable<D, C, V>
{

    public StorageNodeCompoundBase(int[] tupleIdxs, TupleAccessor<D, C> tupleAccessor) {
        super(tupleIdxs, tupleAccessor);
    }

//    public Meta2NodeCompoundBase(Meta2Node<D, C, V> child) {
//        super();
//        this.child = child;
//    }
//
//    @Override
//    public Meta2Node<D, C, V> getChild() {
//        return child;
//    }
}