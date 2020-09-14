package org.apache.jena.dboe.storage.advanced.tuple.trash;

public abstract class IndexNodeBase<ComponentType>
    implements IndexNode<ComponentType>
{
    protected IndexNodeFork<ComponentType> parent;

    public IndexNodeBase(IndexNodeFork<ComponentType> parent) {
        super();
        this.parent = parent;
    }

    @Override
    public IndexNodeFork<ComponentType> getParent() {
        return parent;
    }
}
