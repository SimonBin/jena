package org.apache.jena.dboe.storage.tuple;

import java.util.List;

public interface TupleConstraint<ComponentType> {
    List<ComponentType> getConstraints();
}
