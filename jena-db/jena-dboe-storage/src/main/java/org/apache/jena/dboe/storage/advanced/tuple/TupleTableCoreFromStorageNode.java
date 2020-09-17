package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeBased;

public interface TupleTableCoreFromStorageNode<D, C, V>
    extends TupleTableCore<D, C>, StorageNodeBased<D, C, V>
{
}
