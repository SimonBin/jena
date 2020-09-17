package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
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

    protected int id = -1;

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

    public int getId() {
        return id;
    }

    @Override
    public int depth() {
        return depth;
    }

    @Override
    public int id() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
    public List<StoreAccessorImpl<D, C>> getChildren() {
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


    public static <D, C> StoreAccessorImpl<D, C> createForStorage(StorageNode<D, C, ?> root) {

        // Wrap each storage node with an accessor
        StoreAccessorImpl<D, C> result = TreeLib.<StorageNode<D, C, ?>, StoreAccessorImpl<D, C>>createTreePreOrder(
                null,
                root,
                StorageNode::getChildren,
                StoreAccessorImpl::new,
                StoreAccessorImpl::addChild);

        // Insert extra leaf nodes (with null storage) because
        // childIdx on an accessor refers to chooseSubStore on the parent
        //TODO


        int id = 0;
        for (StoreAccessorImpl<D, C> node : Traverser.<StoreAccessorImpl<D, C>>
            forTree(x -> x.getChildren()).depthFirstPreOrder(result)) {
            node.setId(id++);
        }

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



    /**
     * Cartesian product with support for reducing keys
     * Implicitly assumes that the altIdx is 0
     *
     * TODO Raise an exception when calling this method on a storage node that actually can have multiple
     * alts.
     *
     */
    @Override
    public <T, K> Streamer<?, Entry<K, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K> keyReducer
            ) {
        return cartesianProduct(pattern, accessor, initialAccumulator, keyReducer, 0);
    }


//    @Override
//    public <T, K> Streamer<?, ? extends Entry<K, ?>> cartesianProductOfParent(
//            T pattern,
//            TupleAccessorCore<? super T, ? extends C> accessor,
//            K initialAccumulator,
//            KeyReducer<K> keyReducer
//        ) {
//
//        Streamer<?, ? extends Entry<K, ?>> result = parent == null
//            ? rootStoreAlts -> Stream.of(Maps.immutableEntry(initialAccumulator, rootStoreAlts))
//            : parent.cartesianProduct(pattern, accessor, initialAccumulator, keyReducer, childIndex);
//
//        return result;
//    };
//

    @Override
    public <T, K> Streamer<?, Entry<K, ?>> cartesianProductOfParent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor, K initialAccumulator, IndexedKeyReducer<K> keyReducer) {

        Streamer<?, Entry<K, ?>> result = parent == null
                ? rootStoreAlts -> Stream.of(Maps.immutableEntry(initialAccumulator, rootStoreAlts))
                : parent.cartesianProduct(pattern, accessor, initialAccumulator, keyReducer, childIndex);

        return result;
    }


//    @Override
    public <T, K> Streamer<?, Entry<K, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K> keyReducer,
            int lastAltIdxChoice) {


        StreamTransform<Entry<K, ?>, Entry<K, ?>> streamXform = cartesianProductStreamTransform(
                pattern, accessor, keyReducer, lastAltIdxChoice);

        Streamer<?, Entry<K, ?>> result = rootStoreAlts -> {
            Entry<K, ?> rootEntry = Maps.immutableEntry(initialAccumulator, rootStoreAlts);
            Stream<Entry<K, ?>> rootStream = Stream.of(rootEntry);
            Stream<Entry<K, ?>> r = streamXform.apply(rootStream);
            return r;
        };

        return result;

    }

    public <T, K> StreamTransform<Entry<K, ?>, Entry<K, ?>> cartesianProductStreamTransform(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            IndexedKeyReducer<K> keyReducer,
            int altIdx
        ) {

        Streamer<?, ? extends Entry<?, ?>> thisStreamer =
                storage.streamerForKeyAndSubStoreAlts(pattern, accessor);

        StreamTransform<Entry<K, ?>, Entry<K, ?>> parentXform = parent == null
                ? x -> x
                : parent.cartesianProductStreamTransform(pattern, accessor, keyReducer, childIndex);

        StreamTransform<Entry<K, ?>, Entry<K, ?>> result =
                inStream -> parentXform.apply(inStream)
                .flatMap(keyAccumulatorAndStoreAlts -> {
                    K keyAcumulator = keyAccumulatorAndStoreAlts.getKey();
                    Object storeAlts = keyAccumulatorAndStoreAlts.getValue();

                    Object store = storage.chooseSubStoreRaw(storeAlts, altIdx);

                    Stream<Entry<K, ?>> r = thisStreamer.streamRaw(store)
                        .map(keyContribAndNextStoreAlts -> {
                            Object keyContrib = keyContribAndNextStoreAlts.getKey();
                            Object nextStoreAlts = keyContribAndNextStoreAlts.getValue();

                            K nextKeyAccumulator = keyReducer.reduce(keyAcumulator, depth, keyContrib);

                            return Maps.immutableEntry(nextKeyAccumulator, nextStoreAlts);
                        });
                   return r;
                });

        return result;
    }



    @SuppressWarnings("unchecked")
    @Override
    public <T> Streamer<?, D> streamerForContent(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {

        StorageNode<D, C, ?> storageNode = getStorage();

        Streamer<?, ?> valuesStreamer = storageNode.streamerForValues(pattern, accessor);

        // The cartesian product is invoked on the parent
        Streamer<?, Entry<Object, ?>> cartesianProduct =
                cartesianProductOfParent(pattern, accessor, null, KeyReducers::passOn);

        return store -> cartesianProduct.streamRaw(store).flatMap(e -> {
            Object storeAlts = e.getValue();
            Object valueStore = storageNode.chooseSubStoreRaw(storeAlts, 0);

            // TODO Assumption that the current node holds items of type D
            return valuesStreamer.streamRaw(valueStore).map(x -> (D)x);
        });
    }


    @Override
    public String toString() {
        return "[(node" + id + "; " + storage + ")]";
    }

    @Override
    public <T, K> Streamer<?, K> streamerForKeys(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K> keyReducer) {

        StorageNode<D, C, ?> storageNode = getStorage();

        Streamer<?, ?> keysStreamer = storageNode.streamerForKeys(pattern, accessor);

        Streamer<?, Entry<K, ?>> cartesianProduct =
                cartesianProductOfParent(pattern, accessor, initialAccumulator, keyReducer);

        return store -> cartesianProduct.streamRaw(store).flatMap(e -> {
            K accumulator = e.getKey();
            Object storeAlts = e.getValue();
            Object valueStore = storageNode.chooseSubStoreRaw(storeAlts, 0);
//          Object valueStore = this.getParent().getStorage()ZchooseSubStoreRaw(storeAlts, 0);

            return keysStreamer.streamRaw(valueStore).map(key -> keyReducer.reduce(accumulator, depth, key));
        });
    }



    //
//  for (int i = 0; i < ancestors.size(); ++i) {
//      IndexTreeNodeImpl<D, C> node = ancestors.get(i);
//
//      Streamer<?, ? extends Entry<?, ?>> nextStreamer = node.getStorage().streamerForKeyAndSubStores(pattern, accessor);
//
//      Streamer<? extends Entry<?, ?>, ? extends Entry<?, ?>> prevPairStreamer = currPairStreamer;
//
//
//
//      // Take an argument and pass it to the parent first;
//      // the components of the pair are (key, store)
//      currPairStreamer = argPair -> {
//          Stream<? extends Entry<?, ?>> stream = prevPairStreamer.streamRaw(argPair);
//          return stream.flatMap(e -> {
//              Object subStoreAlts = e.getValue();
//
////              System.out.println("  alts: " + subStoreAlts.getClass() + " - " + subStoreAlts);
//              Object subStore = node.getStorage().chooseSubStoreRaw(subStoreAlts, node.getChildIndex());
//
//              Stream<? extends Entry<?, ?>> subStream = nextStreamer.streamRaw(subStore);
//
//              return subStream.map(e2 -> {
//                  // FIXME Add some mechanisms so that we can skip creating pairs when we are not
//                  // interested in them
//
//                  return Maps.immutableEntry(Maps.immutableEntry(e.getKey(), e2.getKey()), e2.getValue());
//              });
//          });
//      };
//  }
//

//  // The result takes the root store as input - however it needs to be mapped to a
//  // dummy pair where the root store is the value
//  Streamer<Entry<?, ?>, Entry<?, ?>> tmpp = currPairStreamer;
//  return store -> tmpp.streamRaw(Maps.immutableEntry(null, store));
//}


// Overly complex version with needless nested lambdas ; but somehow appared to be working
//public <T> Streamer<?, Entry<?, ?>> cartesianProductOld(
//      T pattern,
//      TupleAccessorCore<? super T, ? extends C> accessor) {
//
//  // Gather this node's ancestors in a list
//  // Root is first element in the list because of depthFirstPostOrder
//  List<StoreAccessorImpl<D, C>> ancestors = ancestors();
//
//  // The lambdas in the following are deliberately verbose in an attempt to ease debugging
//
//  // The root of the cartesian product is an entry where the first store becomes the value of an entry
//  Streamer<Entry<?, ?>, Entry<?, ?>> currPairStreamer = e -> Stream.of(e); //store -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), store));
//
//  for (int i = 0; i < ancestors.size(); ++i) {
//      StoreAccessorImpl<D, C> node = ancestors.get(i);
//
//      Streamer<?, ? extends Entry<?, ?>> nextStreamer = node.getStorage().streamerForKeyAndSubStoreAlts(pattern, accessor);
//
//      Streamer<? extends Entry<?, ?>, ? extends Entry<?, ?>> prevPairStreamer = currPairStreamer;
//
//      // Take an argument and pass it to the parent first;
//      // the components of the pair are (key, store)
//      currPairStreamer = argPair -> {
//          Stream<? extends Entry<?, ?>> stream = prevPairStreamer.streamRaw(argPair);
//          return stream.flatMap(e -> {
//              Object subStoreAlts = e.getValue();
//
////              System.out.println("  alts: " + subStoreAlts.getClass() + " - " + subStoreAlts);
//              Object subStore = node.getStorage().chooseSubStoreRaw(subStoreAlts, node.getChildIndex());
//
//              Stream<? extends Entry<?, ?>> subStream = nextStreamer.streamRaw(subStore);
//
//              return subStream.map(e2 -> {
//                  // FIXME Add some mechanisms so that we can skip creating pairs when we are not
//                  // interested in them
//
//                  return Maps.immutableEntry(Maps.immutableEntry(e.getKey(), e2.getKey()), e2.getValue());
//              });
//          });
//      };
//  }
//
//
//  // The result takes the root store as input - however it needs to be mapped to a
//  // dummy pair where the root store is the value
//  Streamer<Entry<?, ?>, Entry<?, ?>> tmpp = currPairStreamer;
//  return store -> tmpp.streamRaw(Maps.immutableEntry(null, store));
//}





/*

    public <T, K> Streamer<?, ? extends Entry<K, ?>> cartesianProductOld(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            KeyReducer<K> keyReducer,
            int lastAltIdxChoice
            ) {

        // Gather this node's ancestors in a list
        // Root is first element in the list because of depthFirstPostOrder
        List<StoreAccessorImpl<D, C>> ancestors = ancestors();

        List<Streamer<?, ? extends Entry<?, ?>>> nodeStreamers = new ArrayList<>(ancestors.size());
        for (int i = 0; i < ancestors.size(); ++i) {
            StorageNode<D, C, ?> strg = ancestors.get(i).getStorage();
            Streamer<?, ? extends Entry<?, ?>> r = strg.streamerForKeyAndSubStoreAlts(pattern, accessor);
            nodeStreamers.add(r);
        }

        Streamer<?, ? extends Entry<K, ?>>  result = rootStore -> {
            Entry<K, ?> rootEntry = Maps.immutableEntry(initialAccumulator, rootStore);
            return recursiveCartesianProduct(rootEntry, ancestors, nodeStreamers, keyReducer, 0, lastAltIdxChoice);
        };

        return result;
    };


    public <T, K> Stream<Entry<K, ?>> recursiveCartesianProduct(
            Entry<K, ?> keyAccumulatorAndStoreAlts,
            List<StoreAccessorImpl<D, C>> ancestors,
            List<Streamer<?, ? extends Entry<?, ?>>> nodeStreamers,
            KeyReducer<K> keyReducer,
            int i,
            int lastAltIdx) {
        Stream<Entry<K, ?>> result;

        if (i >= ancestors.size()) {
            result = Stream.of(keyAccumulatorAndStoreAlts);
        } else {
            StoreAccessorImpl<D, C> node = ancestors.get(i);

            StorageNode<D, C, ?> storage = node.getStorage();
            Object storeAlts = keyAccumulatorAndStoreAlts.getValue();
            int altIdx = (i + 1 >= ancestors.size()) ? lastAltIdx : ancestors.get(i + 1).getChildIndex();

            Object store = storage.chooseSubStoreRaw(storeAlts, altIdx);


            K keyAcumulator = keyAccumulatorAndStoreAlts.getKey();

            Streamer<?, ? extends Entry<?, ?>> streamer = nodeStreamers.get(i);

            return streamer.streamRaw(store)
                .map(keyContribAndStoreAlts -> {
                    Object keyContrib = keyContribAndStoreAlts.getKey();
                    K nextKeyAccumulator = keyReducer.reduce(keyAcumulator, i, keyContrib);

                    Object nextStoreAlts = keyContribAndStoreAlts.getValue();
                    return Maps.immutableEntry(nextKeyAccumulator, nextStoreAlts);
                })
                .flatMap(e -> recursiveCartesianProduct(e, ancestors, nodeStreamers, keyReducer, i + 1, lastAltIdx));
        }

        return result;
    }
*/
}
