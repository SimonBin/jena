package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.LinkedHashSet;
import java.util.Set;

public class NodeStats<D, C> {
    protected StoreAccessor<D, C> accessor;
    protected com.github.andrewoma.dexx.collection.List<Integer> matchedComponents;
    protected Set<Integer> matchedComponentsSet;

//    public NodeStats(StoreAccessor<D, C> accessor, int[] matches) {
//        super();
//        this.accessor = accessor;
//        this.matches = matches;
//    }

    public NodeStats(StoreAccessor<D, C> accessor, com.github.andrewoma.dexx.collection.List<Integer> matchedComponents) {
        super();
        this.accessor = accessor;
        this.matchedComponents = matchedComponents;
        this.matchedComponentsSet = new LinkedHashSet<>(matchedComponents.asList());
    }



    /** Component indices */
     // protected Set<Integer> matches;

    public com.github.andrewoma.dexx.collection.List<Integer> getMatchedComponents() {
        return matchedComponents;
    }

    public Set<Integer> getMatchedComponentsSet() {
        return matchedComponentsSet;
    }

    public StoreAccessor<D, C> getAccessor() {
        return accessor;
    }



    @Override
    public String toString() {
        return "NodeStats [accessor=" + accessor + ", matchedComponents=" + matchedComponents
                + ", matchedComponentsSet=" + matchedComponentsSet + "]";
    }


}