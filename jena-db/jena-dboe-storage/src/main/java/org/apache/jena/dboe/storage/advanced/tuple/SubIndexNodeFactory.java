package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.dboe.storage.advanced.tuple.trash.IndexNode;
import org.apache.jena.dboe.storage.advanced.tuple.trash.IndexNodeFork;

public interface SubIndexNodeFactory<ComponentType, SubCollectionType> {
    IndexNodeFork<ComponentType> create(
            IndexNode<ComponentType> parent,
            SubCollectionType subType);
}
