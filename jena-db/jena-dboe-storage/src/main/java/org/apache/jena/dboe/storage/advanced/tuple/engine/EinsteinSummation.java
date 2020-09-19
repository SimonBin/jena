package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
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

        Map<C, Integer> varToVarIdx = new HashMap<>();
        for (int r = 0; r < varList.size(); ++r) {
            varToVarIdx.put(varList.get(r), r);
        }


        List<SliceState<D, C>> initialSlices = new ArrayList<>();

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
            SliceState<D, C> tupleSlice = new SliceState<>(rootNode, store, remainingVarIdxs, varIdxToTupleIdxs);

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

        recurse(initialSlices, null, null, tupleAccessor);
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
     * @param remainingVarIdxs
     * @param rootNode
     * @param btp
     * @param tupleAccessor
     */
    public static <D, C, T> void recurse(
            List<SliceState<D, C>> slices,
            Set<Integer> remainingVarIdxs,
            LinkedList<Integer> contextVarIdxs,
            //List<T> btp,
            TupleAccessor<T, C> tupleAccessor) {

        int pickedVarIdx = remainingVarIdxs.iterator().next();

        // Find all slices that project that variable in any of its remaining components
        // Use an identity hash set in case some of the sets turn out to be references to the same set
        Set<Set<C>> valuesForPickedVarIdx = Sets.newIdentityHashSet();
        for (SliceState<D, C> slice : slices) {
            int[] varInSliceComponents = slice.getComponentsForVar(pickedVarIdx);

            if (varInSliceComponents != null) {
                Set<C> valuesContrib = slice.getValuesForVar(pickedVarIdx);
                valuesForPickedVarIdx.add(valuesContrib);
            }
        }


        // Created the intersection of all value sets
        // Sort the contributions by size (smallest first)
        List<Set<C>> valueContrib = new ArrayList<>(valuesForPickedVarIdx);
        Collections.sort(valueContrib, (a, b) -> b.size() - a.size());

        Set<C> remainingValuesOfPickdVar = null;




//        Multimap<String, String> x;
//        x.get("foo").size()






        // slice all nodes by the values of the given var
        for (SliceState<D, C> slice : slices) {


//            Set<C> newSlices = slice.something();

        }



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
    static class SliceState<D, C> {
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

        public SliceState(
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


        /**
         * A variable index may map to multiple tuple indices
         * For example, in
         * [?x, foo, ?x]
         * the variable ?x maps to the indices 0 and 2
         *
         *
         *
         * @param varIdx
         * @param value
         * @return
         */
        public SliceState<D, C> specialize(int varIdx, C value) {
            int[] tupleIdxs = varIdxToTupleIdxs[varIdx];

            Objects.requireNonNull(tupleIdxs, "Var index did not map to an index in the tuple space");

            if (tupleIdxs.length == 2) {
                throw new UnsupportedOperationException("not sure if this case reaelly needs to be handled if we did things smarter");
            }

            int tupleIdx = tupleIdxs[0];

            SliceState<D, C> result = sliceOnComponentWithValue(tupleIdx, value);

            // right, if there are multiple tuple indices for a var, then pick one the specialize
            // on (causing a descend on the index)
            // then recheck the reached values on the other indices

            // still not sure if this has to be done here or on the outside


            return result;
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
        public SliceState<D, C> sliceOnComponentWithValue(int tupleIdx, C sliceKey) {

            SliceState<D, C> result = null;

            Object altStore = null;
            StorageNode<D, C, ?> altNode = null;

            if (storageNode.isAltNode()) {
                // If we are positioned at the root then we assume to be positioned at an alt node
                altStore = store;
                altNode = storageNode;

            } else {
                // otherwise if we assume to be at an innerMap or leafSet/Map node
                // In the case of an innerMap we can descend to its alt node for the given
                // sliceKey
                if (storageNode.isMapNode()) {
                    Map<?, ?> keyToSubStores = storageNode.getStoreAsMap(store);

                    // Find the value in the key set
                    Object subStore = keyToSubStores.get(sliceKey);

                    if (subStore == null) {
                        // Index miss; there is no such tuple with that component; return a null slice
                        altNode = null;
                    } else {
                        altNode = storageNode.getChildren().get(0);
                    }
                }
            }


            if (altNode != null) {
                int childIdx = findChildThatIndexesByTupleIdx(altNode, tupleIdx);
                StorageNode<D, C, ?> nextStorage = altNode.getChildren().get(childIdx);
                Object nextStore = altNode.chooseSubStoreRaw(altStore, childIdx);

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


                result = new SliceState<>(nextStorage, nextStore, nextRemainingVarIdxs, varIdxToTupleIdxs);
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

        public Set<C >getValuesForVar(int varIdx) {
            return null;
        }

        public Set<C> streamValues() {
            return null;
//            Streamer<V, C> streamer = storageNode.streamerForValues(null, idx -> null);
//            Stream<C> result = streamer.stream(store);
//            return result;
        }


        /**
         * Assumption: The start node has a single child that an alt node
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
