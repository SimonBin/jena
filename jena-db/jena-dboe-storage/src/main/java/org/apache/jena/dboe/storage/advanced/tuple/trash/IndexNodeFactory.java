package org.apache.jena.dboe.storage.advanced.tuple.trash;

public interface IndexNodeFactory<ComponentType> {
    IndexNode<ComponentType> create(IndexNodeFork<ComponentType> parent);
}
