package org.apache.jena.dboe.storage.advanced.tuple;

public interface SubIndexNodeFactory<ComponentType, SubCollectionType> {
    IndexNodeFork<ComponentType> create(
            IndexNode<ComponentType> parent,
            SubCollectionType subType);
}
