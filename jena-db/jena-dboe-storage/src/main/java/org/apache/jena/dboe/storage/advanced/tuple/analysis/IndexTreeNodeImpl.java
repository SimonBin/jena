package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.ext.com.google.common.collect.Maps;
import org.apache.jena.ext.com.google.common.graph.Traverser;


public class IndexTreeNodeImpl<D, C>
//    implements IndexTreeNode
{
    protected Meta2Node<D, C, ?> storage;
    protected IndexTreeNodeImpl<D, C> parent;
    protected int depth = 0;
    protected int childIndex = 0; // The ith child of the parent
    protected List<IndexTreeNodeImpl<D, C>> children = new ArrayList<>();

    public IndexTreeNodeImpl(
            Meta2Node<D, C, ?> storage,
            IndexTreeNodeImpl<D, C> parent) {
        super();
        this.storage = storage;
        this.parent = parent;
        this.depth = parent == null ? 0 : parent.depth + 1;
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

    public <T> Streamer<?, Entry<?, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {

        // Root is first element in the list because of depthFirstPostOrder
        List<IndexTreeNodeImpl<D, C>> ancestors = Lists.newArrayList(Traverser.<IndexTreeNodeImpl<D, C>>forTree(n -> n.getParent() == null
                ? Collections.emptySet()
                : Collections.singleton(n.getParent())).depthFirstPostOrder(this));


        // The lambdas in the following are deliberately verbose in an attempt to ease debugging

        // The root of the cartesian product is an entry where the first store becomes the value of an entry
        Streamer<Entry<?, ?>, Entry<?, ?>> currPairStreamer = e -> Stream.of(e); //store -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), store));

        for (int i = 0; i < ancestors.size(); ++i) {
            IndexTreeNodeImpl<D, C> node = ancestors.get(i);

            Streamer<?, ? extends Entry<?, ?>> nextStreamer = node.getStorage().streamerForKeyAndSubStores(pattern, accessor);

            Streamer<? extends Entry<?, ?>, ? extends Entry<?, ?>> prevPairStreamer = currPairStreamer;

            // Take an argument and pass it to the parent first;
            // the components of the pair are (key, store)
            currPairStreamer = argPair -> {
                Stream<? extends Entry<?, ?>> stream = prevPairStreamer.streamRaw(argPair);
                return stream.flatMap(e -> {
                    Object subStoreAlts = e.getValue();

//                    System.out.println("  alts: " + subStoreAlts.getClass() + " - " + subStoreAlts);
                    Object subStore = node.getStorage().chooseSubStoreRaw(subStoreAlts, node.getChildIndex());

                    Stream<? extends Entry<?, ?>> subStream = nextStreamer.streamRaw(subStore);

                    return subStream.map(e2 -> {
                        return Maps.immutableEntry(Maps.immutableEntry(e.getKey(), e2.getKey()), e2.getValue());
                    });
                });
            };
        }


        // The result takes the root store as input - however it needs to be mapped to a
        // dummy pair where the root store is the value
        Streamer<Entry<?, ?>, Entry<?, ?>> tmpp = currPairStreamer;
        return store -> tmpp.streamRaw(Maps.immutableEntry(null, store));
    }

    @Override
    public String toString() {
        return "[(" + depth + ", " + childIndex + "); " + storage + ")]";
    }

    //void cart()


}
