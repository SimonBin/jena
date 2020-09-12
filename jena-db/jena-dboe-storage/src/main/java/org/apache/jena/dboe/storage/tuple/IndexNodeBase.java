package org.apache.jena.dboe.storage.tuple;

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
