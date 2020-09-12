package org.apache.jena.dboe.storage.tuple;

public interface SubIndexNodeFactory<ComponentType, SubCollectionType> {
    IndexNodeFork<ComponentType> create(
            IndexNode<ComponentType> parent,
            SubCollectionType subType);
}
