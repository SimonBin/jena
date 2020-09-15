package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import org.apache.jena.atlas.lib.persistent.PSet;
import org.apache.jena.atlas.lib.persistent.PersistentSet;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;

public class IndexPathReport {
    protected int depth;

    protected IndexPathReport parent;
    protected int childId;
    protected PersistentSet<Integer> matchedComponents;
    protected StorageNode<?, ?, ?> indexNode;

    public IndexPathReport(IndexPathReport parent, int childId) {
        this(parent, childId, null, PSet.empty());
    }

    public IndexPathReport(IndexPathReport parent, int childId, StorageNode<?, ?, ?> indexNode, PersistentSet<Integer> matchedComponents) {
        super();
        this.depth = parent == null ? 0 : parent.depth + 1;
        this.parent = parent;
        this.childId = childId;
        this.indexNode = indexNode;
        this.matchedComponents = matchedComponents;
    }

    public IndexPathReport getParent() {
        return parent;
    }

    public int getChildId() {
        return childId;
    }

    public PersistentSet<Integer> getMatchedComponents() {
        return matchedComponents;
    }

    public StorageNode<?, ?, ?> getIndexNode() {
        return indexNode;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return "PathReport [parent=" + parent + ", childId=" + childId + ", matchedComponents=" + matchedComponents.asSet()
                + "]";
    }
}
