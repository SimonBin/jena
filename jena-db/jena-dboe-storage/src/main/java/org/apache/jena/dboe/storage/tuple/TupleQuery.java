package org.apache.jena.dboe.storage.tuple;

import java.util.List;

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

    void setProject(int... tupleIdxs);
}
