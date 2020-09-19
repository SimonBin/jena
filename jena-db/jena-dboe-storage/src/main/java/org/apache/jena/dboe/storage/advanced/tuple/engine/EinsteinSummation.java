package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;


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
            int varIdxToTupleIdxs[][] = new int[varDim][];

            for (int i = 0; i < tupleDim; ++i) {
                C value = tupleAccessor.get(tuple, i);
                Integer varIdx = varToVarIdx.get(value);
                if (varIdx != null) {
                    // varIdxToTupleIdx
                }
            }
            SliceState<D, C> tupleSlice = new SliceState<>(store, rootNode, varIdxToTupleIdxs);

            // specialize the tuples in the btp by the mentioned constants
            for (int i = 0; i < tupleDim; ++i) {
                C value = tupleAccessor.get(tuple, i);

                // if value is not a variable then..
                if (!varToVarIdx.containsKey(value)) {
                    tupleSlice = tupleSlice.specializeByTupleIdx(i, value);

                    if (tupleSlice == null) {
                        // a constant mentioned in the bgp is not found in an index -
                        // yield an empty result set
                        initialSlices.clear();
                        break outer;
                    }
                }

                initialSlices.add(tupleSlice);
            }
        }



        // recurse()
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
    public <D, C, T> void recurse(
            List<SliceState<D, C>> slices,
            Set<Integer> remainingVarIdxs,
            LinkedList<Integer> contextVarIdxs,
            //List<T> btp,
            TupleAccessor<T, C> tupleAccessor) {

        int varIdx = remainingVarIdxs.iterator().next();



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
//        protected StoreAccessor<D, C> accessor;

        protected int[][] varIdxToTupleIdxs;


        public SliceState(Object store, StorageNode<D, C, ?> storageNode, int[][] varIdxToTupleIdxs) {
            super();
            this.store = store;
            this.storageNode = storageNode;
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

            SliceState<D, C> result = specializeByTupleIdx(tupleIdx, value);

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
        public SliceState<D, C> specializeByTupleIdx(int tupleIdx, C value) {

            StorageNode<D, C, ?> altNode = storageNode.getChildren().get(0);
            int childIdx = findChildThatIndexesByTupleIdx(altNode, tupleIdx);





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
            return null;
        }

        // TODO Returning the collection of components from a leaf collection would given
        // more optimization potential - especially when the backing sets are identical
//        public Set<C> getValues() {
//
//        }

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

    }


}
