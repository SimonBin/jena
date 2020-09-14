package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.List;
import java.util.Map.Entry;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;


/**
 * Auxiliary tree structure wrapping a storage node structure
 * Features cartesian products and projection of keys
 *
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 */
public interface IndexTreeNode<D, C> {
    IndexTreeNode<D, C> getParent();

    List<? extends IndexTreeNode<D, C>> getChildren();
    IndexTreeNode<D, C> child(int idx);
    int childCount();

    Meta2Node<D, C, ?> getStorage();

    // reflexive
    IndexTreeNode<D, C> leastNestedChildOrSelf();


    <T> Streamer<?, Entry<?, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor);

}
