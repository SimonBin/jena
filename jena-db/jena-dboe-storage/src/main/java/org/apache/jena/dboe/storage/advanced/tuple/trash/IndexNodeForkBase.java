package org.apache.jena.dboe.storage.advanced.tuple.trash;

import org.apache.jena.dboe.storage.advanced.tuple.TupleConstraint;

public abstract class IndexNodeForkBase<ComponentType>
    implements IndexNodeFork<ComponentType>
{
    protected IndexNode<ComponentType> parent;
    protected TupleConstraint<ComponentType> constraints;

    public IndexNodeForkBase(IndexNode<ComponentType> parent) {
        this(parent, null);
    }

    public IndexNodeForkBase(IndexNode<ComponentType> parent, TupleConstraint<ComponentType> constraints) {
        super();
        this.parent = parent;
        this.constraints = constraints;
    }

    @Override
    public IndexNode<ComponentType> getParent() {
        return parent;
    }

    @Override
    public TupleConstraint<ComponentType> getConststraintContribution() {
        return constraints;
    }

    @Override
    public TupleConstraint<ComponentType> getOveralConstraints() {
        // collect the constraints from this and all ancestors
        return null;
    }
}
