package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;
import java.util.Set;

public interface TupleQuery<ComponentType> {
    /**
     * The rank of the conceptual tuple table this query is intended for
     *
     * @return
     */
    int getRank();

    void setDistinct(boolean onOrOff);
    boolean isDistinct();

    void setConstraint(int idx, ComponentType value);
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

    void setProject(int... tupleIdxs);
}
