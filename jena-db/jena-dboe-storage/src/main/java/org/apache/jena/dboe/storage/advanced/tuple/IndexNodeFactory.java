package org.apache.jena.dboe.storage.advanced.tuple;

public interface IndexNodeFactory<ComponentType> {
    IndexNode<ComponentType> create(IndexNodeFork<ComponentType> parent);
}
