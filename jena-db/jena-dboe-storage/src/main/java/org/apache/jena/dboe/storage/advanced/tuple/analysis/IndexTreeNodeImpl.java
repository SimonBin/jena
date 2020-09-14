package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

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
    protected int childIndex = -1; // The ith child of the parent
    protected List<IndexTreeNodeImpl<D, C>> children = new ArrayList<>();

    public IndexTreeNodeImpl(
            Meta2Node<D, C, ?> storage,
            IndexTreeNodeImpl<D, C> parent) {
        super();
        this.storage = storage;
        this.parent = parent;
    }

    public IndexTreeNodeImpl<D, C> child(int idx) {
        return children.get(idx);
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

    public int getChildIndex() {
        return childIndex;
    }

    public void addChild(IndexTreeNodeImpl<D, C> child) {
        child.childIndex = children.size();
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

        // The root of the cartesian product is an entry where the first store becomes the value of an entry
        Streamer<?, ? extends Entry<?, ?>> current = null; //store -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), store));
        IndexTreeNodeImpl<D, C> parentNode = null;
        for (int i = 0; i < ancestors.size(); ++i) {
            IndexTreeNodeImpl<D, C> node = ancestors.get(i);

            Streamer<?, ? extends Entry<?, ?>> next = node.getStorage().streamerForKeyAndSubStores(pattern, accessor);
            if (current == null) {
                current = next;
            } else {
                Streamer<?, ? extends Entry<?, ?>> tmp = current;
                Meta2Node<D, C, ?> parentStorage = parentNode.getStorage();
                current = store -> tmp.streamRaw(store).flatMap(
                        e -> {
                            Object subStoreAlts = e.getValue();
                            Object subStore = parentStorage.chooseSubStoreRaw(subStoreAlts, node.getChildIndex());

                            return next.streamRaw(subStore).map(e2 -> Maps.immutableEntry(e.getKey(), e2.getValue()));
                        });
            }

            parentNode = node;
        }


        return current;
    }

    //void cart()

}
