package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;

public class TupleConstraintImpl<ComponentType>
    implements TupleConstraint<ComponentType>
{
    protected List<ComponentType> constraints;

    public TupleConstraintImpl(List<ComponentType> constraints) {
        super();
        this.constraints = constraints;
    }

    public List<ComponentType> getConstraints() {
        return constraints;
    }
}
