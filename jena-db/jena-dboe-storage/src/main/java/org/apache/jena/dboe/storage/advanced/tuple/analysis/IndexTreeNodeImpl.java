package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.ext.com.google.common.graph.Traverser;

import com.github.jsonldjava.shaded.com.google.common.collect.Maps;


public class IndexTreeNodeImpl<D, C>
//    implements IndexTreeNode
{
    protected Meta2Node<D, C, ?> storage;
    protected IndexTreeNodeImpl<D, C> parent;
    protected List<IndexTreeNodeImpl<D, C>> children = new ArrayList<>();

    public IndexTreeNodeImpl(
            Meta2Node<D, C, ?> storage,
            IndexTreeNodeImpl<D, C> parent) {
        super();
        this.storage = storage;
        this.parent = parent;
    }

    public static <D, C> IndexTreeNodeImpl<D, C> bakeTree(Meta2Node<D, C, ?> root) {

        IndexTreeNodeImpl<D, C> result = TreeLib.<Meta2Node<D, C, ?>, IndexTreeNodeImpl<D, C>>createTreePreOrder(
                null,
                root,
                Meta2Node::getChildren,
                IndexTreeNodeImpl::new,
                IndexTreeNodeImpl::addChild);

        return result;
    }

    public IndexTreeNodeImpl<D, C> getParent() {
        return parent;
    }

    public Meta2Node<D, C, ?> getStorage() {
        return storage;
    }

    public void addChild(IndexTreeNodeImpl<D, C> child) {
        children.add(child);
    }




    /**
     * Create a streamer for the component at
     *
     * @param key
     * @param forwardingFunction
     */
//    public <TupleLike, ComponentType> Streamer<Object, > streamerForComponent(
//        TupleLike pattern,
//        TupleAccessorCore<TupleLike, ComponentType> accessor) {
//
//
//
//
//    }

    public <T> Streamer<?, ? extends Entry<?, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {

        // Root is first element in the list because of depthFirstPostOrder
        List<IndexTreeNodeImpl<D, C>> ancestors = Lists.newArrayList(Traverser.<IndexTreeNodeImpl<D, C>>forTree(n -> n.getParent() == null
                ? Collections.emptySet()
                : Collections.singleton(n.getParent())).depthFirstPostOrder(this));

//        List<Streamer<?, ? extends Entry<?, ?>>> streamers = ancestors.stream()
//                .map(node -> node.getStorage().streamerForEntries(pattern, accessor))
//                .collect(Collectors.toList());

        // Streamer<?, ?> result = null;

        Streamer<?, ? extends Entry<?, ?>> current = null;
        for (int i = 0; i < ancestors.size(); ++i) {
            IndexTreeNodeImpl<D, C> node = ancestors.get(i);

            Streamer<?, ? extends Entry<?, ?>> next = node.getStorage().streamerForEntries(pattern, accessor);
            if (current == null) {
                current = next;
            } else {
                Streamer<?, ? extends Entry<?, ?>> tmp = current;
                current = store -> tmp.streamRaw(store)
                        .flatMap(e -> next.streamRaw(e.getValue()).map(v -> Maps.immutableEntry(e.getKey(), v)));
            }
        }


        return current;
    }

    //void cart()

}
