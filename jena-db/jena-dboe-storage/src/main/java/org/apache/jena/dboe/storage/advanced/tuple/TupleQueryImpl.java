package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleQueryImpl<ComponentType>
    implements TupleQuery<ComponentType>
{
    protected int rank;
    protected List<ComponentType> pattern;
    protected boolean distinct = false;
    protected int[] projection = null;

    public static <T> List<T> listOfNulls(int size) {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < size; ++i) { result.add(null); }
        return result;
    }

    public TupleQueryImpl(int rank) {
        super();
        this.rank = rank;
        this.pattern = listOfNulls(rank);
    }

    @Override
    public int getRank() {
        return rank;
    }

    @Override
    public void setDistinct(boolean onOrOff) {
        this.distinct = onOrOff;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public void setConstraint(int idx, ComponentType value) {
        pattern.set(idx, value);
    }

    @Override
    public ComponentType getConstraint(int idx) {
        return pattern.get(idx);
    }
    @Override
    public int[] getProject() {
        return projection;
    }

    @Override
    public List<ComponentType> getPattern() {
        return pattern;
    }

    @Override
    public void setProject(int... tupleIdxs) {
        projection = tupleIdxs;
    }

    @Override
    public boolean hasProject() {
        return projection != null;
    }
}
