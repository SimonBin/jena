package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.ext.com.google.common.collect.Maps;
import org.apache.jena.ext.com.google.common.graph.Traverser;


public class StoreAccessorImpl<D, C>
    implements StoreAccessor<D, C>
{
    protected StorageNode<D, C, ?> storage;
    protected StoreAccessorImpl<D, C> parent;
    protected int depth = 0;
    protected int childIndex = 0; // The ith child of the parent
    protected List<StoreAccessorImpl<D, C>> children = new ArrayList<>();


    // Caches (computed on first request)
    protected StoreAccessorImpl<D, C> leastNestedNode = null;
    protected List<StoreAccessorImpl<D, C>> ancestorsCache = null;


    public StoreAccessorImpl(
            StorageNode<D, C, ?> storage,
            StoreAccessorImpl<D, C> parent) {
        super();
        this.storage = storage;
        this.parent = parent;
        this.depth = parent == null ? 0 : parent.depth + 1;
    }

    @Override
    public StoreAccessorImpl<D, C> child(int idx) {
        return children.get(idx);
    }

    @Override
    public int childCount() {
        return children.size();
    }

    @Override
    public List<? extends StoreAccessor<D, C>> getChildren() {
        return children;
    }


    @Override
    public StoreAccessor<D, C> leastNestedChildOrSelf() {
        if (leastNestedNode == null) {
            leastNestedNode = (StoreAccessorImpl<D, C>) Meta2NodeLib.findLeastNestedIndexNode(this);
        }

        return leastNestedNode;
    }

    @Override
    public List<StoreAccessorImpl<D, C>> ancestors() {
        if (ancestorsCache == null) {
            ancestorsCache = Lists.newArrayList(Traverser.<StoreAccessorImpl<D, C>>forTree(n -> n.getParent() == null
                    ? Collections.emptySet()
                    : Collections.singleton(n.getParent())).depthFirstPostOrder(this));
        }

        return ancestorsCache;
    }


    public static <D, C> StoreAccessorImpl<D, C> createForStore(StorageNode<D, C, ?> root) {

        StoreAccessorImpl<D, C> result = TreeLib.<StorageNode<D, C, ?>, StoreAccessorImpl<D, C>>createTreePreOrder(
                null,
                root,
                StorageNode::getChildren,
                StoreAccessorImpl::new,
                StoreAccessorImpl::addChild);

        return result;
    }


    @Override
    public StoreAccessorImpl<D, C> getParent() {
        return parent;
    }


    @Override
    public StorageNode<D, C, ?> getStorage() {
        return storage;
    }

    public int getChildIndex() {
        return childIndex;
    }

    public void addChild(StoreAccessorImpl<D, C> child) {
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


    @Override
    public <T, K> Streamer<?, ? extends Entry<K, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            KeyReducer<K> keyReducer
            ) {

        // Gather this node's ancestors in a list
        // Root is first element in the list because of depthFirstPostOrder
        List<StoreAccessorImpl<D, C>> ancestors = ancestors();

        List<Streamer<?, ? extends Entry<?, ?>>> nodeStreamers = ancestors.stream().map(
                node -> node.getStorage().streamerForKeyAndSubStores(pattern, accessor))
                .collect(Collectors.toList());

        Streamer<?, ? extends Entry<K, ?>>  result = rootStore -> {
            Entry<K, ?> rootEntry = Maps.immutableEntry(initialAccumulator, rootStore);
            return recursiveCartesianProduct(rootEntry, ancestors, nodeStreamers, keyReducer, 0);
        };

        return result;
    };


    /**
     * Input is a pair whose first element is the keys assembled so far
     * and the second one is the next one corresponds to store alternatives
     *
     * @param <T>
     * @param keysAndStoreAlts
     * @param ancestors
     * @param nodeStreamers
     * @param i
     * @return
     */
    public <T, K> Stream<Entry<K, ?>> recursiveCartesianProduct(
            Entry<K, ?> keysAndStoreAlts,
            List<StoreAccessorImpl<D, C>> ancestors,
            List<Streamer<?, ? extends Entry<?, ?>>> nodeStreamers,
            KeyReducer<K> keyReducer,
            int i) {
        Stream<Entry<K, ?>> result;

        if (i >= ancestors.size()) {
            result = Stream.of(keysAndStoreAlts);
        } else {
            StoreAccessorImpl<D, C> node = ancestors.get(i);
            Streamer<?, ? extends Entry<?, ?>> nextStreamer = nodeStreamers.get(i);

            Object storeAlts = keysAndStoreAlts.getValue();
            Object store = node.getStorage().chooseSubStoreRaw(storeAlts, node.getChildIndex());

            return nextStreamer.streamRaw(store)
                .map(e2 -> {
                    K priorKey = keysAndStoreAlts.getKey();
                    Object currentKey = e2.getKey();
                    K reducedKey = keyReducer.reduce(priorKey, i, currentKey);
                    return Maps.immutableEntry(reducedKey, e2.getValue());
                })
                .flatMap(e -> recursiveCartesianProduct(e, ancestors, nodeStreamers, keyReducer, i + 1));
        }

        return result;
    }


//
//        for (int i = 0; i < ancestors.size(); ++i) {
//            IndexTreeNodeImpl<D, C> node = ancestors.get(i);
//
//            Streamer<?, ? extends Entry<?, ?>> nextStreamer = node.getStorage().streamerForKeyAndSubStores(pattern, accessor);
//
//            Streamer<? extends Entry<?, ?>, ? extends Entry<?, ?>> prevPairStreamer = currPairStreamer;
//
//
//
//            // Take an argument and pass it to the parent first;
//            // the components of the pair are (key, store)
//            currPairStreamer = argPair -> {
//                Stream<? extends Entry<?, ?>> stream = prevPairStreamer.streamRaw(argPair);
//                return stream.flatMap(e -> {
//                    Object subStoreAlts = e.getValue();
//
////                    System.out.println("  alts: " + subStoreAlts.getClass() + " - " + subStoreAlts);
//                    Object subStore = node.getStorage().chooseSubStoreRaw(subStoreAlts, node.getChildIndex());
//
//                    Stream<? extends Entry<?, ?>> subStream = nextStreamer.streamRaw(subStore);
//
//                    return subStream.map(e2 -> {
//                        // FIXME Add some mechanisms so that we can skip creating pairs when we are not
//                        // interested in them
//
//                        return Maps.immutableEntry(Maps.immutableEntry(e.getKey(), e2.getKey()), e2.getValue());
//                    });
//                });
//            };
//        }
//

//        // The result takes the root store as input - however it needs to be mapped to a
//        // dummy pair where the root store is the value
//        Streamer<Entry<?, ?>, Entry<?, ?>> tmpp = currPairStreamer;
//        return store -> tmpp.streamRaw(Maps.immutableEntry(null, store));
//    }


    // Overly complex version with needless nested lambdas ; but somehow appared to be working
    public <T> Streamer<?, Entry<?, ?>> cartesianProductOld(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {

        // Gather this node's ancestors in a list
        // Root is first element in the list because of depthFirstPostOrder
        List<StoreAccessorImpl<D, C>> ancestors = ancestors();

        // The lambdas in the following are deliberately verbose in an attempt to ease debugging

        // The root of the cartesian product is an entry where the first store becomes the value of an entry
        Streamer<Entry<?, ?>, Entry<?, ?>> currPairStreamer = e -> Stream.of(e); //store -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), store));

        for (int i = 0; i < ancestors.size(); ++i) {
            StoreAccessorImpl<D, C> node = ancestors.get(i);

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
                        // FIXME Add some mechanisms so that we can skip creating pairs when we are not
                        // interested in them

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
