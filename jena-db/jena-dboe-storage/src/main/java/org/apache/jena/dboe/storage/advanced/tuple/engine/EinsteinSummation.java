package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.ext.com.google.common.collect.HashMultiset;
import org.apache.jena.ext.com.google.common.collect.Multiset;
import org.apache.jena.ext.com.google.common.collect.Sets;


public class EinsteinSummation {


    /**
     *
     * This triple pattern executed in micro(!)seconds on tentris and took ~2 seconds using conventional
     * join - so let's try to see where the magic comes from.
     * The traditional way to join is to iterate the triple patterns and performing nested lookups.
     *
     * The better way seems to be to iterate
     * the variables and somehow does some set intersection across all triple patterns.
     *
     * Thereby nested indexes can be exploited by saving the nested state.
     *
     * ?a  a ?b
     * ?c  a ?d
     * ?a ?e ?c
     *
     *
     * @param <D>
     * @param <C>
     * @param <T>
     * @param <V> The index structure;
     *            (typically some nested lists, maps, sets, etc List<Map<Map<>>, Map<>>)
     * @param btp A basic tuple pattern (as a generalization of a basic graph pattern)
     * @param tupleAccessor
     * @param isVar Test whether a value for C is a variable
     */
    public static <D, C, T>  void einsum(
            StorageNode<D, C, ?> rootNode,
            Object store,
            Iterable<T> btp,
            TupleAccessor<T, C> tupleAccessor,
            Predicate<C> isVar)
    {
        // First find out the set of variables
        // (Hm, maybe we should use C for components in the store and another generic CV
        // for the type of components with variables used in lookup tuple patterns)
        Set<C> vars = new LinkedHashSet<>();

        // Get the dimension of tuples from the storage
        rootNode.getTupleAccessor().getDimension();

        int tupleDim = tupleAccessor.getDimension();
        for (T tuple : btp) {
            for (int i = 0; i < tupleDim; ++i) {
                C c = tupleAccessor.get(tuple, i);
                if (isVar.test(c)) {
                    vars.add(c);
                }
            }
        }


        // set up a vector of variable indices
        int varDim = vars.size();
        List<C> varList = new ArrayList<>(vars);
        int[] varIdxs = new int[varDim];

        Map<C, Integer> varToVarIdx = new HashMap<>();
        for (int r = 0; r < varDim; ++r) {
            varToVarIdx.put(varList.get(r), r);
            varIdxs[r] = r;
        }


        List<SliceNode<D, C>> initialSlices = new ArrayList<>();

        // Set up the initial Slice states for the tuples
        outer: for (T tuple : btp) {

            // Note: The mapping array spans across all variables; unused fields are simply left null
            // in essence its a cheap form of a Multimap<VarIdx, TupleIdx>
            // As the number of vars is usually quite small this should cause no problems
            int remainingVarIdxs[] = new int[0];
            int varIdxToTupleIdxs[][] = new int[varDim][];

            for (int i = 0; i < tupleDim; ++i) {
                C value = tupleAccessor.get(tuple, i);
                Integer varIdx = varToVarIdx.get(value);
                if (varIdx != null) {
                    remainingVarIdxs = ArrayUtils.add(remainingVarIdxs, varIdx);
                    varIdxToTupleIdxs[varIdx] = ArrayUtils.add(varIdxToTupleIdxs[varIdx], i);
                }
            }
            SliceNode<D, C> tupleSlice = new SliceNode<>(rootNode, store, remainingVarIdxs, varIdxToTupleIdxs);

            // specialize the tuples in the btp by the mentioned constants
            for (int i = 0; i < tupleDim; ++i) {
                C value = tupleAccessor.get(tuple, i);

                // if value is not a variable then..
                if (!varToVarIdx.containsKey(value)) {
                    tupleSlice = tupleSlice.sliceOnComponentWithValue(i, value);

                    if (tupleSlice == null) {
                        // a constant mentioned in the bgp is not found in an index -
                        // yield an empty result set
                        initialSlices.clear();
                        break outer;
                    }
                }
            }
            initialSlices.add(tupleSlice);
        }

        List<Multiset<C>> varIdxToValues = new ArrayList<Multiset<C>>(varDim);
        for (int i = 0; i < varDim; ++i) {
            varIdxToValues.add(HashMultiset.create());
        }


        recurse(varIdxs, initialSlices, varIdxToValues);
    }

    /**
     * So now we first pick a variable by which to slice,
     * then we (conceptually) create the intersection of its values in every slice,
     *
     *
     * @param <D>
     * @param <C>
     * @param <T>
     * @param <V>
     * @param remainingVarIdxs The remaining varIdxs by which slicing is possible across all slices
     * @param rootNode
     * @param btp
     * @param tupleAccessor
     */
    public static <D, C, T> boolean recurse(
            int[] remainingVarIdxs,
            List<SliceNode<D, C>> slices,
            List<Multiset<C>> varIdxToValues) {

        // Indicate empty solution set for this variable until we find at least one
        boolean result = false;

        // Find out which variable to pick
        // For each varIdx iterate all slices and find out the minimum and maximum number of valuee


        for (SliceNode<D, C> slice : slices) {
            // slice.getValuesForComponent(tupleIdx)
        }

        // FIXME - Hack just pick one
        int pickedVarIdx = remainingVarIdxs[0];
        int[] nextRemainingVarIdxs = ArrayUtils.remove(remainingVarIdxs, pickedVarIdx);

        // Find all slices that project that variable in any of its remaining components
        // Use an identity hash set in case some of the sets turn out to be references to the same set
        Set<Set<C>> valuesForPickedVarIdx = Sets.newIdentityHashSet();
        for (SliceNode<D, C> slice : slices) {
            int[] varInSliceComponents = slice.getComponentsForVar(pickedVarIdx);

            if (varInSliceComponents != null) {
                for(int tupleIdx : varInSliceComponents) {
                    Set<C> valuesContrib = slice.getValuesForComponent(tupleIdx); //pickedVarIdx);
                    valuesForPickedVarIdx.add(valuesContrib);
                }
            }
        }


        // Created the intersection of all value sets
        // Sort the contributions by size (smallest first)
        List<Set<C>> valueContrib = new ArrayList<>(valuesForPickedVarIdx);
        Collections.sort(valueContrib, (a, b) -> b.size() - a.size());


        Set<C> remainingValuesOfPickedVarTmp = valueContrib.stream().reduce(Sets::intersection).orElse(null);
        if (remainingValuesOfPickedVarTmp != null) {
            List<SliceNode<D, C>> nextSlices = null;
            // Materialize the intersection
            Set<C> remainingValuesOfPickedVar = new HashSet<>(remainingValuesOfPickedVarTmp);
            for (C value : remainingValuesOfPickedVar) {

                nextSlices = slices.stream()
                    .map(slice -> slice.sliceOnVarIdxAndValue(pickedVarIdx, value))
                    // There is no flatMap with nulls; creating intermediate streams seems like a waste
                    .filter(newSlice -> newSlice != null)
                    .collect(Collectors.toList());

                boolean subResult = recurse(nextRemainingVarIdxs, nextSlices, varIdxToValues);
                if (subResult) {
                    varIdxToValues.get(pickedVarIdx).add(value);
                }
            }

            result = nextSlices.isEmpty();
        }

        return result;
    }


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
     * @param <D>
     * @param <C>
     * @param <V>
     */
    static class SliceNode<D, C> {
        protected Object store;
        protected StorageNode<D, C, ?> storageNode;

        protected int[] remainingVars;

        /**
         * The mapping of variable indices to the tuple idxs they belong to
         * [?x, foo, ?x]
         * the variable ?x maps to the indices 0 and 2
         *
         */
        protected int[][] varIdxToTupleIdxs;

        public SliceNode(
                /* link to parent? */
                StorageNode<D, C, ?> storageNode,
                Object store,
                int[] remainingVars,
                int[][] varIdxToTupleIdxs) {
            super();
            this.store = store;
            this.storageNode = storageNode;
            this.remainingVars = remainingVars;
            this.varIdxToTupleIdxs = varIdxToTupleIdxs;
        }

        public int[] getRemainingVars() {
            return remainingVars;
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
            SliceNode<D, C> result;
            if (ArrayUtils.contains(remainingVars, varIdx)) {

                int[] tupleIdxs = varIdxToTupleIdxs[varIdx];

                // Slice by the component with fewer immediate remaining values (immediate means that we do not
                // count the values on the leaf nodes)
                int bestMatchTupleIdx = 0;
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

        public static class NodeAndStore<D, C> {
            protected StorageNode<D, C, ?> storage;
            protected Object store;

            public NodeAndStore(StorageNode<D, C, ?> storage, Object store) {
                super();
                this.storage = storage;
                this.store = store;
            }

            public StorageNode<D, C, ?> getStorage() {
                return storage;
            }

            public Object getStore() {
                return store;
            }
        }

        public static <D, C> NodeAndStore<D, C> findStorageNodeThatIndexesByComponentIdx(
                int tupleIdx,
                StorageNode<D, C, ?> storageNode,
                Object store) {

            StorageNode<D, C, ?> indexNode = null;
            Object indexStore = null;

            if (storageNode.isAltNode()) {
                // If we are positioned at the root then we assume to be positioned at an alt node
                int childIdx = findChildThatIndexesByTupleIdx(storageNode, tupleIdx);
                indexNode = storageNode.getChildren().get(childIdx);
                indexStore = storageNode.chooseSubStoreRaw(store, childIdx);

            } else {
                // We assume to be at a leaf which is implicitly an alt1 node with itself as the child
                indexNode = storageNode;
                indexStore = store;
            }

            return new NodeAndStore<>(indexNode, indexStore);
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

            NodeAndStore<D, C> index = findStorageNodeThatIndexesByComponentIdx(tupleIdx, storageNode, store);
            StorageNode<D, C, ?> indexNode = index.getStorage();
            Object indexStore = index.getStore();

            if (storageNode.isAltNode()) {
                // If we are positioned at the root then we assume to be positioned at an alt node
                int childIdx = findChildThatIndexesByTupleIdx(storageNode, tupleIdx);
                indexNode = storageNode.getChildren().get(childIdx);
                indexStore = storageNode.chooseSubStoreRaw(store, childIdx);

            } else {
                // We assume to be at a leaf which is implicitly an alt1 node with itself as the child
                indexNode = storageNode;
                indexStore = store;
            }


            StorageNode<D, C, ?> nextStorage = null;
            Object nextStore = null;

            // otherwise if we assume to be at an innerMap or leafSet/Map node
            // In the case of an innerMap we can descend to its alt node for the given
            // sliceKey
            if (indexNode.isMapNode()) {
                Map<?, ?> keyToSubStores = storageNode.getStoreAsMap(indexStore);

                // Find the value in the key set
                nextStore = keyToSubStores.get(sliceKey);

                if (nextStore != null) {
                    nextStorage = storageNode.getChildren().get(0);
                }
                // else { Index miss; there is no such tuple with that component; returns a null slice }
            } else if (indexNode.isSetNode()) {
                Set<?> keyToSubStores = storageNode.getStoreAsSet(indexStore);

                // Find the value in the key set
                nextStore = keyToSubStores.contains(sliceKey);

                // Just remain at the leaf; a leaf is identified when there are no more remaining variables
                if (nextStore != null) {
                    nextStorage = storageNode;
                }
            } else {
                throw new IllegalStateException("Expected either a map or set node; got " + storageNode.getClass());
            }


            if (nextStorage != null) {

                // Find the variable that mapped to that tuple (if any*) and remove it from
                // remaining vars
                // * slicing by a constant in a tuple does not affect variables
                int[] nextRemainingVarIdxs = remainingVars.clone();
                for (int i = 0; i < remainingVars.length; ++i) {
                    int varIdx = remainingVars[i];
                    int tupleIdxs[] = varIdxToTupleIdxs[varIdx];
                    if (ArrayUtils.contains(tupleIdxs, tupleIdx)) {
                        nextRemainingVarIdxs = ArrayUtils.removeElement(nextRemainingVarIdxs, varIdx);
                        break;
                    }
                }

                result = new SliceNode<>(nextStorage, nextStore, nextRemainingVarIdxs, varIdxToTupleIdxs);
            }

            return result;



            // map the resultVarIdx to tupleIdx
            // e.g. (?a ?b ?c) -> ?a is in subject position, so ?a maps to tuple index 0
            // actually, just specialize on the tupleIdx and handle the var mapping on the outside

            // Find a child that can handle tupleIdx
            //storageNode.constrainChild(tupleIdx, value);

            // Advance the storageNode and the store to it



            // storageNode.str
            // BreadthFirstSearchLib.breadthFirstFindFirst(storageNode::getChildren, successorFunction, predicate)


            // accessor.getStorage()
            // storageNode.get
        }


        public int[] getComponentsForVar(int varIdx) {
            return varIdxToTupleIdxs[varIdx];
        }


        // TODO Returning the collection of components from a leaf collection would given
        // more optimization potential - especially when the backing sets are identical
//        public Set<C> getValues() {
//
//        }

        /**
         * Find the child that indexes by tupleIdx and return its set of values
         *
         */
        @SuppressWarnings("unchecked")
        public Set<C> getValuesForComponent(int tupleIdx) {

            NodeAndStore<D, C> index = findStorageNodeThatIndexesByComponentIdx(tupleIdx, storageNode, store);
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
            return "Slice (" + Arrays.toString(remainingVars) + " "  + Arrays.deepToString(varIdxToTupleIdxs) + " " + storageNode.getClass().getSimpleName() + ")";
        }
    }


}
