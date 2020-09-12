package org.apache.jena.dboe.storage.tuple;

public interface IndexNodeFactory<ComponentType> {
    IndexNode<ComponentType> create(IndexNodeFork<ComponentType> parent);
}
