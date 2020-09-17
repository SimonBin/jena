package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;
import java.util.Set;

public interface TupleQuery<ComponentType> {
    /** The dimension (number of columns) of the conceptual tuple table this query is intended for */
    int getDimension();

    TupleQuery<ComponentType> setDistinct(boolean onOrOff);
    boolean isDistinct();

    TupleQuery<ComponentType> setConstraint(int idx, ComponentType value);
    ComponentType getConstraint(int idx);

    Set<Integer> getConstrainedComponents();

    List<ComponentType> getPattern();

    /**
     * Baseline tuple query execution on a tuple table.
     * Invokes find(...) on the tupleTable and only afterwards
     * applies filtering, projection and distinct on the obtained stream
     *
     *
     * @return A mutable array for configuration of the projection
     */
    int[] getProject();

    boolean hasProject();

    TupleQuery<ComponentType> setProject(int... tupleIdxs);
}
