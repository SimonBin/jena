package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface IndexTreeNode<V, Self extends IndexTreeNode<V, Self>> {
    IndexTreeNode<V, Self> getParent();
    V getValue();

}
