package org.apache.jena.dboe.storage.tuple;

public class TupleQueryImpl<ComponentType>
    implements TupleQuery<ComponentType>
{
    protected int rank;
    protected ComponentType[] pattern;
    protected boolean distinct = false;
    protected int[] projection = null;

    @SuppressWarnings("unchecked")
    public TupleQueryImpl(int rank) {
        super();
        this.rank = rank;
        this.pattern = (ComponentType[])new Object[rank];
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
        pattern[idx] = value;
    }

    @Override
    public ComponentType getConstraint(int idx) {
        return pattern[idx];
    }
    @Override
    public int[] getProject() {
        return projection;
    }

    @Override
    public ComponentType[] getPattern() {
        return pattern;
    }

    @Override
    public void setProject(int... tupleIdxs) {
        projection = tupleIdxs;
    }
}
