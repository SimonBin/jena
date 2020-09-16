package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.LinkedHashSet;
import java.util.Set;

public class NodeStats<D, C> {
    protected StoreAccessor<D, C> accessor;
    protected com.github.andrewoma.dexx.collection.List<Integer> matchedConstraintIdxs;
    protected com.github.andrewoma.dexx.collection.List<Integer> matchedProjectIdxs;

    protected Set<Integer> matchedConstraintIdxSet;
    protected Set<Integer> matchedProjectIdxSet;

//    public NodeStats(StoreAccessor<D, C> accessor, int[] matches) {
//        super();
//        this.accessor = accessor;
//        this.matches = matches;
//    }

    public NodeStats(
            StoreAccessor<D, C> accessor,
            com.github.andrewoma.dexx.collection.List<Integer> matchedConstraintIdxs,
            com.github.andrewoma.dexx.collection.List<Integer> matchedProjectIdxs) {
        super();
        this.accessor = accessor;
        this.matchedConstraintIdxs = matchedConstraintIdxs;
        this.matchedProjectIdxs = matchedProjectIdxs;

        this.matchedConstraintIdxSet = new LinkedHashSet<>(matchedConstraintIdxs.asList());
        this.matchedProjectIdxSet = new LinkedHashSet<>(matchedProjectIdxs.asList());
    }



    /** Component indices */
     // protected Set<Integer> matches;

    public com.github.andrewoma.dexx.collection.List<Integer> getMatchedConstraintIdxs() {
        return matchedConstraintIdxs;
    }

    public com.github.andrewoma.dexx.collection.List<Integer> getMatchedProjectIdxs() {
        return matchedProjectIdxs;
    }

    public Set<Integer> getMatchedConstraintIdxSet() {
        return matchedConstraintIdxSet;
    }

    public Set<Integer> getMatchedProjectIdxSet() {
        return matchedProjectIdxSet;
    }

    public StoreAccessor<D, C> getAccessor() {
        return accessor;
    }



    @Override
    public String toString() {
        return "NodeStats [accessor=" + accessor + ", matchedConstraintIdxs=" + matchedConstraintIdxs
                + ", matchedProjectIdxs=" + matchedProjectIdxs + ", matchedConstraintIdxSet=" + matchedConstraintIdxSet
                + ", matchedProjectIdxSet=" + matchedProjectIdxSet + "]";
    }

}