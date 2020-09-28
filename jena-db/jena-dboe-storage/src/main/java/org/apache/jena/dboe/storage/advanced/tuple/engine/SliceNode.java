package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;

/**
 * Conceptually slice of a tensor.
 *
 *
 * Backed by a storage node which should form a suitable trie (TODO as described in the paper)
 *
 *
 * For example for a triple pattern (?a ?p ?c) WHERE ?p = rdf:type
 * initially the storage is positioned at the root node.
 * the tuple-index-to-var-index is a vector
 * (ivaridx(?a), varidx(?b), varidx(?c))
 *
 *
 *
 *
 *
 *
 * @author raven
 *
 * @param <D> Domain tuple type - not needed and should be removed
 * @param <C>
 * @param <V>
 */
public class SliceNode<D, C> {
    protected Object store;
    protected StorageNode<D, C, ?> storageNode;

//  These attributes are not needed in the computation; but may be useful for debugging
//    protected int[] initialVarIdxs;
//    protected Object[] varIdxToSliceValue;


    protected int[] remainingVarIdxs;


    /**
     * The mapping of variable indices to the tuple idxs they belong to
     * [?x, foo, ?x]
     * the variable ?x maps to the indices 0 and 2
     *
     */
    protected int[][] varIdxToTupleIdxs;

    public boolean hasRemainingVarIdx(int varIdx) {
        return ArrayUtils.contains(remainingVarIdxs, varIdx);
    }


//    public int[] getInitialVarIdxs() {
//        return initialVarIdxs;
//    }
//
//    public Object[] getVarIdxToSliceValue() {
//        return varIdxToSliceValue;
//    }


    public static <D, C> SliceNode<D, C> create(
            StorageNode<D, C, ?> storageNode,
            Object store,
            int[] remainingVars,
            int[][] varIdxToTupleIdxs
        ) {
        return new SliceNode<D, C>(
                storageNode,
                store,
//                remainingVars.clone(),
//                new Object[remainingVars.length],
                remainingVars,
                varIdxToTupleIdxs
        );
    }

    public SliceNode(
            /* link to parent? */
            StorageNode<D, C, ?> storageNode,
            Object store,
//            int[]    initialVarIdxs,
//            Object[] initialVarIdxToSliceValue,
            int[] remainingVars,
            int[][] varIdxToTupleIdxs) {
        super();
        this.store = store;
//        this.initialVarIdxs = initialVarIdxs;
//        this.varIdxToSliceValue = initialVarIdxToSliceValue;
        this.storageNode = storageNode;
        this.remainingVarIdxs = remainingVars;
        this.varIdxToTupleIdxs = varIdxToTupleIdxs;
    }

    public int[] getRemainingVars() {
        return remainingVarIdxs;
    }


    /**
     * A variable index may map to multiple tuple indices
     * For example, in
     * [?x, foo, ?x]
     * the variable ?x maps to the indices 0 and 2
     *
     * Returns itself if varIdx is not in remainingVarIdxs
     * (We do not track whether a slice was already slice by a varIdx;
     * The assumption is that a slice will never be slice more than once by one varIdx)
     *
     *
     * @param varIdx
     * @param value
     * @return
     */
    public SliceNode<D, C> sliceOnVarIdxAndValue(int varIdx, C value) {
        if(remainingVarIdxs.length == 0) {
            throw new RuntimeException("its empty here");
        }

        SliceNode<D, C> result;
        if (ArrayUtils.contains(remainingVarIdxs, varIdx)) {

            int[] tupleIdxs = varIdxToTupleIdxs[varIdx];

            // Slice by the component with fewer immediate remaining values (immediate means that we do not
            // count the values on the leaf nodes)
            int bestMatchTupleIdx = tupleIdxs[0];
            if (tupleIdxs.length > 1) {
                int bestMatchSize = 0;
                for (int tupleIdx : tupleIdxs) {
                    Set<?> values = getValuesForComponent(tupleIdx);
                    int valuesSize = values.size();
                    if(valuesSize > bestMatchSize) {
                        bestMatchSize = valuesSize;
                        bestMatchTupleIdx = tupleIdx;
                    }
                }
            }

            result = sliceOnComponentWithValue(bestMatchTupleIdx, value);
        } else {
            result = this;
        }
        return result;
    }

    public static <D, C> StorageNodeAndStore<D, C> findStorageNodeThatIndexesByComponentIdx(
            int tupleIdx,
            StorageNode<D, C, ?> storageNode,
            Object store) {

        StorageNode<D, C, ?> indexNode = null;
        Object indexStore = null;

        if (storageNode.isAltNode()) {
            // If we are positioned at the root or an inner then we assume to be positioned at an alt node
            int childIdx = findChildThatIndexesByTupleIdx(storageNode, tupleIdx);
            indexNode = storageNode.getChildren().get(childIdx);
            indexStore = storageNode.chooseSubStoreRaw(store, childIdx);

        } else {
            // We assume to be at a leaf which is implicitly an alt1 node with itself as the child
            indexNode = storageNode;
            indexStore = store;

            int[] keyTupleIdx = indexNode.getKeyTupleIdxs();
            if (keyTupleIdx.length != 1 || keyTupleIdx[0] != tupleIdx) {
                throw new RuntimeException("Should not happen");
            }

        }

        return new StorageNodeAndStore<>(indexNode, indexStore);
    }

    /**
     * Specialize a slice on a key (= a component value) on a certain dimension
     * If there is an entry for that key in the underlying store, returns a new slice state set to the
     * store (sub-trie) of that entry's value.
     * If there is no such entry return null.
     *
     *
     *
     * @param resultVarIdx
     * @param value
     * @return
     */
    public SliceNode<D, C> sliceOnComponentWithValue(int tupleIdx, C sliceKey) {

        SliceNode<D, C> result = null;

        StorageNodeAndStore<D, C> index = findStorageNodeThatIndexesByComponentIdx(tupleIdx, storageNode, store);
        StorageNode<D, C, ?> indexNode = index.getStorage();
        Object indexStore = index.getStore();

        StorageNode<D, C, ?> nextStorage = null;
        Object nextStore = null;

        // otherwise if we assume to be at an innerMap or leafSet/Map node
        // In the case of an innerMap we can descend to its alt node for the given
        // sliceKey
        if (indexNode.isMapNode()) {
            Map<?, ?> keyToSubStores = indexNode.getStoreAsMap(indexStore);

            // Find the value in the key se)t
            nextStore = keyToSubStores.get(sliceKey);

            if (nextStore != null) {
                nextStorage = indexNode.getChildren().get(0);
            }
            // else { Index miss; there is no such tuple with that component; returns a null slice }
        } else if (indexNode.isSetNode()) {
            Set<?> keyToSubStores = indexNode.getStoreAsSet(indexStore);

            // Find the value in the key set
            nextStore = keyToSubStores.contains(sliceKey)
                    ? keyToSubStores
                    : null;

            // Just remain at the leaf; a leaf is identified when there are no more remaining variables
            if (nextStore != null) {
                nextStorage = storageNode;
            }
        } else {
            throw new IllegalStateException("Expected either a map or set node; got " + storageNode.getClass());
        }


        if (nextStorage != null) {

            int[] nextRemainingVarIdxs = removeRemainingVarByTupleIdx(tupleIdx, remainingVarIdxs, varIdxToTupleIdxs);

            result = new SliceNode<>(
                  nextStorage, nextStore,
                  nextRemainingVarIdxs, varIdxToTupleIdxs);

        }

        return result;
    }


    /**
     * The mapping of variable indices to the tuple idxs they belong to
     * [?x, foo, ?x]
     * the variable ?x maps to the indices 0 and 2
     *
     */
    public static int[] removeRemainingVarByTupleIdx(int tupleIdx, int[] remainingVarIdxs, int[][] varIdxToTupleIdxs) {
        // Find the variable that mapped to that tuple (if any*) and remove it from
        // remaining vars
        // * Components of a tuple are not required to map to variables
        int affectedVarIdPos = -1;
        for (int i = 0; i < remainingVarIdxs.length; ++i) {
            int varIdx = remainingVarIdxs[i];
            int tupleIdxs[] = varIdxToTupleIdxs[varIdx];
            if (ArrayUtils.contains(tupleIdxs, tupleIdx)) {
                affectedVarIdPos = i;
                break;
            }
        }


        int[] nextRemainingVarIdxs = affectedVarIdPos == -1
                ? remainingVarIdxs
                : ArrayUtils.remove(remainingVarIdxs, affectedVarIdPos);
        return nextRemainingVarIdxs;
    }


    public int[] getComponentsForVar(int varIdx) {
        int[] result = ArrayUtils.contains(remainingVarIdxs, varIdx)
            ? varIdxToTupleIdxs[varIdx]
            : null
            ;

        return result;
    }


    // TODO Returning the collection of components from a leaf collection would given
    // more optimization potential - especially when the backing sets are identical
//        public Set<C> getValues() {
//
//        }

    /**
     * If a varIdx maps to multiple tuple indices return the smallest set
     *
     * @param varIdx
     * @return
     */
    public Set<C> getSmallestValueSetForVarIdx(int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() < result.size() ? candidate : result);
        }


        return result;
    }

    public Set<C> getLargestValueSetForVarIdx(int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() > result.size() ? candidate : result);
        }


        return result;
    }


    /**
     * Find the child that indexes by tupleIdx and return its set of values
     *
     */
    @SuppressWarnings("unchecked")
    public Set<C> getValuesForComponent(int tupleIdx) {

        StorageNodeAndStore<D, C> index = findStorageNodeThatIndexesByComponentIdx(tupleIdx, storageNode, store);
        StorageNode<D, C, ?> indexNode = index.getStorage();
        Object indexStore = index.getStore();

        Set<C> result = null;

        // In the case of a map return its key set
        // For  leafComponentSets return the set
        if (indexNode.isMapNode()) {
            Map<?, ?> map = indexNode.getStoreAsMap(indexStore);
            result = (Set<C>)map.keySet();
        } else if (indexNode.isSetNode()) {
            Set<?> set = indexNode.getStoreAsSet(indexStore);
            result = (Set<C>)set;
        } else {
            throw new IllegalStateException("map or set node expected - got: " + indexNode);
        }

        return result;
    }


    /**
     * Assumption: The start node either is alreaedy a leaf
     * (implicitly an alt1 with just itself as a child)
     * Pick the child from the alt node that indexes by the given tupleIdx
     *
     *
     * @param <D>
     * @param <C>
     * @param <V>
     * @param altNode
     * @param tupleIdx
     * @return
     */
    public static <D, C> int findChildThatIndexesByTupleIdx(StorageNode<D, C, ?> altNode, int tupleIdx) {

        // Pick the alternative with the right tupleIdx
        int result = -1;


        List<? extends StorageNode<D, C, ?>> children = altNode.getChildren();
        for (int j = 0; j < children.size(); ++j) {
            StorageNode<D, C, ?> child = children.get(j);

            int[] tupleIdxs = child.getKeyTupleIdxs();
            if (tupleIdxs.length == 1 && tupleIdxs[0] == tupleIdx) {
                result = j;
                break;
            }
        }

        if (result == -1) {
            throw new RuntimeException("structure does not match the assumed trie");
        }

        return result;
    }

    @Override
    public String toString() {
        return "Slice (" + Arrays.toString(remainingVarIdxs) + " "  + Arrays.deepToString(varIdxToTupleIdxs) + " " + storageNode.getClass().getSimpleName() + ")";
    }
}