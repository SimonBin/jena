package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import org.apache.jena.atlas.lib.persistent.PSet;
import org.apache.jena.atlas.lib.persistent.PersistentSet;

public class PathReport {
    protected PathReport parent;
    protected int childId;
    protected PersistentSet<Integer> matchedComponents;

    public PathReport(PathReport parent, int childId) {
        this(parent, childId, PSet.empty());
    }

    public PathReport(PathReport parent, int childId, PersistentSet<Integer> matchedComponents) {
        super();
        this.parent = parent;
        this.childId = childId;
        this.matchedComponents = matchedComponents;
    }

    public PathReport getParent() {
        return parent;
    }

    public int getChildId() {
        return childId;
    }

    public PersistentSet<Integer> getMatchedComponents() {
        return matchedComponents;
    }

    @Override
    public String toString() {
        return "PathReport [parent=" + parent + ", childId=" + childId + ", matchedComponents=" + matchedComponents.asSet()
                + "]";
    }
}
