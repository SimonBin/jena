package org.apache.jena.dboe.storage.tuple;

import java.util.Arrays;

public class ConstraintBuilderBase<ComponentType>
    implements ConstraintBuilder<ComponentType>
{
    protected IndexNode<ComponentType> parent;

    // TODO Replace with a List<Range<NodeCmpWrapper<Node>>> - so we need to wrap Node with Comparable
    // in order to use it with guava Range
    protected TupleConstraint<ComponentType> constraints;
    //protected ConstraintTypeO

    public ConstraintBuilderBase(IndexNode<ComponentType> parent) {
        super();
        this.parent = parent;
    }

    @Override
    public IndexNode<ComponentType> getParent() {
        return parent;
    }

    @Override
    public ConstraintBuilder<ComponentType> set(ComponentType... componentTypes) {
        if (getParent().getRank() != componentTypes.length) {
            throw new IllegalArgumentException("Mismatch in ranks"); // TODO include values in message
        }

        constraints = new TupleConstraintImpl<>(Arrays.asList(componentTypes));

        return this;
    }

    @Override
    public IndexNodeFork<ComponentType> build() {
        return parent.forConstraints(constraints);
    }

}
