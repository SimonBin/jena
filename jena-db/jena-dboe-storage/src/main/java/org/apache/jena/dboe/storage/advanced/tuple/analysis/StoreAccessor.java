/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.List;
import java.util.Map.Entry;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.Streamer;


/**
 * Storage retrievers form an auxiliary tree structure wrapping a storage nodes
 *
 * Features cartesian products and projection of keys
 *
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 */
public interface StoreAccessor<D, C> {
    StoreAccessor<D, C> getParent();

    List<? extends StoreAccessor<D, C>> getChildren();
    StoreAccessor<D, C> child(int idx);
    int childCount();
    int depth();
    int id();


    StorageNode<D, C, ?> getStorage();


    /**
     * If the wrapped storage node is the first one starting from the root that
     * projects tuple idxs then these indices are considered unique.
     *
     * Hm, actually, we shoulud consider constraints; and allow to 'precompile' with a lambda
     * an instance of the state of the node under constraints.
     * If all ancestors under a constraint have a known cardinality of at most 1 (and at least 1)
     * and the index wrappeded by this node uses unique keys
     *
     *
     *
     *
     *
     * @return
     */
//    default boolean isTupleIdxUnique() {
//
//    }

    /**
     * Yields the lest of ancestors of this node. The first item in the list
     * will be the root and the last one is the node on which this method was invoked.
     *
     * @return
     */
    List<? extends StoreAccessor<D, C>> ancestors();


    /**
     * Return the least nested child (or this) which <b>holds domain tuples</b>.
     *
     * @return
     */
    StoreAccessor<D, C> leastNestedChildOrSelf();


    /**
     * Return streamer that performs a cartesian product on the parent for a given store.
     * If the streamer is created for the root storage accesssor then its invocation
     * with 'rootStore' and 'initialAccumulator' returns a single "identity" entry of the
     * form (initialAccumulator, rootStore).
     *
     *
     *
     * @param <T>
     * @param <K>
     * @param pattern
     * @param accessor
     * @param initialAccumulator
     * @param keyReducer
     * @return
     */
    <T, K> Streamer<?, Entry<K, ?>> cartesianProductOfParent(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K, Object> keyReducer);


    /**
     * Create the cartesian product from the root until this node under
     * given equality constraints
     *
     * The returned streamer takes as input the root's corresponding store
     * and yields {@link Entry} objects whose key is the keys of the indices as nested entries
     * and the value is the store object that can be fed into this node's storage node.
     *
     * @param <T>
     * @param pattern
     * @param accessor
     * @return
     */
    <T, K> Streamer<?, Entry<K, ?>> cartesianProduct(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K, Object> keyReducer
            );



    public <T> Streamer<?, D> streamerForContent(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor);


    public <T, K> Streamer<?, K> streamerForKeys(
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor,
            K initialAccumulator,
            IndexedKeyReducer<K, Object> keyReducer
            );

    static <D, C> StoreAccessor<D, C> findLeastNestedIndexNode(StoreAccessor<D, C> node) {
        return BreadthFirstSearchLib.breadthFirstFindFirst(
                node,
                StoreAccessor::getChildren,
                n -> n.childCount() == 0);
    }

}
