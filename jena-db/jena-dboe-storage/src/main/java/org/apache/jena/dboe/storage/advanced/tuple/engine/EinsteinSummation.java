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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.BiReducer;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.IndexedKeyReducer;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.ext.com.google.common.collect.HashMultiset;
import org.apache.jena.ext.com.google.common.collect.Multiset;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;


public class EinsteinSummation {


    /**
     * Einsum with a tuple codec
     *
     * @param storage
     * @param store
     * @param bgp
     * @param projectVars
     * @return
     */
//    public static <C> Stream<Binding> einsum(
//            TupleCodec<Triple, Node, ?, C> tupleCodec,
//            StorageNode<?, C, ?> storage,
//            Object store,
//            BasicPattern bgp,
//            Set<Var> projectVars)
//    {
//        // Encode the BGP
////    	for (Triple triple :)
//
//
//        BiReducer<Binding, Node, Node> reducer = projectVars == null
//                ? (binding, varNode, valueNode) -> BindingFactory.binding(binding, (Var)varNode, valueNode)
//                // Skip binding creation of non-projected vars in order to save a few CPU cycles
//                : (binding, varNode, valueNode) -> (projectVars.contains(varNode)
//                        ? BindingFactory.binding(binding, (Var)varNode, valueNode)
//                        : binding);
//
//        Stream<Binding> result =
//        EinsteinSummation.einsum(
//                storage,
//                store,
//                bgp.getList(),
//                tupleCodec::getEncodedComponent,
//                Node::isVariable,
//                BindingFactory.root(),
//                reducer);
//
//        return result;
//    }

    public static Stream<Binding> einsum(
            StorageNode<?, Node, ?> storage,
            Object store,
            BasicPattern bgp,
            Set<Var> projectVars)
    {
        BiReducer<Binding, Node, Node> reducer = projectVars == null
                ? (binding, varNode, valueNode) -> BindingFactory.binding(binding, (Var)varNode, valueNode)
                // Skip binding creation of non-projected vars in order to save a few CPU cycles
                : (binding, varNode, valueNode) -> (projectVars.contains(varNode)
                        ? BindingFactory.binding(binding, (Var)varNode, valueNode)
                        : binding);

        Stream<Binding> result =
        EinsteinSummation.einsumGeneric(
                storage,
                store,
                bgp.getList(),
                TupleAccessorTripleAnyToNull.INSTANCE,
                Node::isVariable,
                Function.identity(),
                Function.identity(),
                BindingFactory.root(),
                reducer);

        return result;
    }

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
    public static <C0, C, T, A>  Stream<A> einsumGeneric(
            StorageNode<?, C, ?> rootNode,
            Object store,
            Iterable<T> btp,
            TupleAccessor<T, C0> tupleAccessor,
            Predicate<C0> isVar, // In general we cannot pass variables through the encoder so we need a predicate
            Function<C0, C> componentEncoder, // E.g. map concrete nodes to integers via a dictionary
            Function<C, C0> componentDecoder, // E.g. map integers back to nodes via a dictionary
            A initialAccumulator,
            BiReducer<A, C0, C0> reducer // first C = variable, second C = value; both as Nodes
            )
    {
        // First find out the set of variables
        Set<C0> vars = new LinkedHashSet<>();

        // Get the dimension of tuples from the storage
        rootNode.getTupleAccessor().getDimension();

        int tupleDim = tupleAccessor.getDimension();
        for (T tuple : btp) {
            for (int i = 0; i < tupleDim; ++i) {
                C0 c = tupleAccessor.get(tuple, i);
                if (isVar.test(c)) {
                    vars.add(c);
                }
            }
        }

        // set up a vector of variable indices
        int varDim = vars.size();
        List<C0> varList = new ArrayList<>(vars);
        int[] varIdxs = new int[varDim];

        // Use var = varList[varIdx] for the reverse mapping
        Map<C0, Integer> varToVarIdx = new HashMap<>();
        for (int r = 0; r < varDim; ++r) {
            varToVarIdx.put(varList.get(r), r);
            varIdxs[r] = r;
        }


        List<SliceNode<?, C>> initialSlices = new ArrayList<>();

        // Set up the initial Slice states for the tuples
        outer: for (T tuple : btp) {

            // Note: The mapping array spans across all variables; unused fields are simply left null
            // in essence its a cheap form of a Multimap<VarIdx, TupleIdx>
            // As the number of vars is usually quite small this should cause no problems
            int remainingVarIdxs[] = new int[0];
            int varIdxToTupleIdxs[][] = new int[varDim][];

            for (int i = 0; i < tupleDim; ++i) {
                C0 rawValue = tupleAccessor.get(tuple, i);
                Integer varIdx = varToVarIdx.get(rawValue);

                if (varIdx != null) {
                    remainingVarIdxs = ArrayUtils.add(remainingVarIdxs, varIdx);
                    varIdxToTupleIdxs[varIdx] = ArrayUtils.add(varIdxToTupleIdxs[varIdx], i);
                }
            }
            SliceNode<?, C> tupleSlice = SliceNode.create(rootNode, store, remainingVarIdxs, varIdxToTupleIdxs);

            // specialize the tuples in the btp by the mentioned constants
            for (int i = 0; i < tupleDim; ++i) {
                C0 rawValue = tupleAccessor.get(tuple, i);

                // if value is not a variable then..
                if (!varToVarIdx.containsKey(rawValue)) {
                    C value = componentEncoder.apply(rawValue);

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

        // Wrap the incoming reducer with another one that maps var indices to the actual vars
        IndexedKeyReducer<A, C> varIdxBasedReducer = (acc, varIdx, encodedValue) -> {
            C0 var = varList.get(varIdx);
            C0 decodedValue = componentDecoder.apply(encodedValue);
            return reducer.reduce(acc, var, decodedValue);
        };


        // Do not execute anything unless an operation is invoked on the stream
        Stream<A> stream = Stream.of(initialAccumulator).flatMap(acc -> recurse(
                varDim,
                varIdxs,
                initialSlices,
                acc,
                varIdxBasedReducer));

        return stream;
    }






    /**
     *
     * @param <D> The domain type of tuples such as Triple. Not needed in the algo.
     * @param <C> The type of the domain tuple's values; e.g. Node
     * @param <A> The type of the accumulator used for the construction of result objects
     * @param varDim The number of variables; respectively their indices
     * @param remainingVarIdxs The indices of the variables not yet processed
     * @param slices The list of slices to process
     * @param accumulator The accumulator to which intermediate contributions via the reducer are made. E.g. Binding
     * @param reducer Used to blend intermediate solutions (variable idx, value) with the accumulator
     *        in order to obtain a new accumulator.
     *
     * @return A stream of the final accumulators
     */
    public static <C, A> Stream<A> recurse(
            int varDim,
            int[] remainingVarIdxs,
            List<SliceNode<?, C>> slices,
            A accumulator,
            IndexedKeyReducer<A, C> reducer // receives varIdx and value
            ) {

//        boolean debug = true;
//        if (debug) System.out.println("Recursion started: RemainingVarIdxs: " + Arrays.toString(remainingVarIdxs) + " numSlices=" + slices.size());
//        if (slices == null || slices.isEmpty()) {
//            return false;
//        }
//
        if (remainingVarIdxs.length == 0) {
//            return IntStream.rangeClosed(0, slices.size()).mapToObj(foo -> accumulator);
            return Stream.of(accumulator);
        }

        // Find out which variable to pick
        // For each varIdx iterate all slices and find out the minimum and maximum number of valuee
        int pickedVarIdx = findBestSliceVarIdx(varDim, remainingVarIdxs, slices);


        int[] nextRemainingVarIdxs = ArrayUtils.removeElement(remainingVarIdxs, pickedVarIdx);
//        if (debug) System.out.println("Picked var " + pickedVarIdx + " among " + Arrays.toString(remainingVarIdxs) + " with now remaining " + Arrays.toString(nextRemainingVarIdxs));

        // Find all slices that project that variable in any of its remaining components
        // Use an identity hash set in case some of the sets turn out to be references to the same set
        Set<Set<C>> valuesForPickedVarIdx = Sets.newIdentityHashSet();
        for (SliceNode<?, C> slice : slices) {
            int[] varInSliceComponents = slice.getComponentsForVar(pickedVarIdx);

            if (varInSliceComponents != null) {
                for(int tupleIdx : varInSliceComponents) {
                    Set<C> valuesContrib = slice.getValuesForComponent(tupleIdx);

//                    if (valuesContrib.isEmpty()) {
//                        System.out.println("should never happen");
//                    }

                    valuesForPickedVarIdx.add(valuesContrib);
                }
            }
        }

        // Created the intersection of all value sets
        // Sort the contributions by size (smallest first)

        Set<C> remainingValuesOfPickedVar;

        switch (valuesForPickedVarIdx.size()) {
        case 0:
            remainingValuesOfPickedVar = Collections.emptySet();
            break;
        case 1:
            remainingValuesOfPickedVar = valuesForPickedVarIdx.iterator().next();
            break;
        default: // (valueContribs.size() > 1)
            List<Set<C>> valueContribs = new ArrayList<>(valuesForPickedVarIdx);

            // The sorting of the sets by size is VERY important!
            // Consider computing the intersection between two sets of sizes 100.000 and 1
            // We do not want to copy 100K values just to retain 1
            Collections.sort(valueContribs, (a, b) -> a.size() - b.size());

            remainingValuesOfPickedVar = new HashSet<>(valueContribs.get(0));
            for (int i = 1; i < valueContribs.size(); ++i) {
                Set<C> contrib = valueContribs.get(i);
                remainingValuesOfPickedVar.retainAll(contrib);
            }
            break;
        }

        List<SliceNode<?, C>> sliceableByPickedVar = new ArrayList<>();
        List<SliceNode<?, C>> nonSliceableByPickedVar = new ArrayList<>();

        for (SliceNode<?, C> slice : slices) {
            if (slice.hasRemainingVarIdx(pickedVarIdx)) {
                sliceableByPickedVar.add(slice);
            } else {
                nonSliceableByPickedVar.add(slice);
            }
        }

        return remainingValuesOfPickedVar.stream().flatMap(value -> {
            return sliceByValue(
                    varDim,
                    nextRemainingVarIdxs,
                    accumulator,
                    reducer,
                    pickedVarIdx,
                    sliceableByPickedVar,
                    nonSliceableByPickedVar,
                    value);
        });
    }


    public static <C, K> Stream<K> sliceByValue(
            int varDim,
            int[] nextRemainingVarIdxs,
            K accumulator,
            IndexedKeyReducer<K, C> reducer,
            int pickedVarIdx,
            List<SliceNode<?, C>> sliceableByPickedVar,
            List<SliceNode<?, C>> nonSliceableByPickedVar,
            C value) {


        // Perhaps Interables.concat?
        List<SliceNode<?, C>> allNextSlices = new ArrayList<>(nonSliceableByPickedVar);

//        if (nonSliceableByPickedVar.size() > 2) {
//            System.out.println("Non-sliceable " + nonSliceableByPickedVar.size() + " sliceable " + sliceableByPickedVar.size());
//
//        }

        for (SliceNode<?, C> slice : sliceableByPickedVar) {
            SliceNode<?, C> nextSlice = slice.sliceOnVarIdxAndValue(pickedVarIdx, value);

            if (nextSlice != null) {
                if (nextSlice.getRemainingVars().length != 0) {
                    allNextSlices.add(nextSlice);
                }
            }

        }

        K nextAccumulator = reducer.reduce(accumulator, pickedVarIdx, value);

        // All slices that mentioned a certain var are now constrained to one of the
        // var's value
        return recurse(varDim, nextRemainingVarIdxs, allNextSlices, nextAccumulator, reducer);
    }

    public static <C>  int findBestSliceVarIdx(
            int varDim,
            int[] remainingVarIdxs,
            List<SliceNode<?, C>> slices) {
        // The score is a reduction factor - the higher the better
        int bestVarIdx = remainingVarIdxs[0];

        if (remainingVarIdxs.length > 1) {
//            int varDim = varIdxToValues.size();
            int[] mins = new int[varDim];
            // int[] maxs = new int[varDim];

            @SuppressWarnings("unchecked")
            Set<Integer>[] varIdxToInvolvedSetSizes = (Set<Integer>[])new Set[varDim];
            for (int varIdx : remainingVarIdxs) {
                varIdxToInvolvedSetSizes[varIdx] = new HashSet<>();
            }

            float[] varToScore = new float[varDim];

            Arrays.fill(mins, Integer.MAX_VALUE);
            Arrays.fill(varToScore, 1.0f);
            // Arrays.fill(varIdxToNumDifferentSizes, 0);
//            Arrays.fill(maxs, 0);

            for (SliceNode<?, C> slice : slices) {

                for (int varIdx : slice.getRemainingVars()) {
                    Set<C> minSet = slice.getSmallestValueSetForVarIdx(varIdx);
                    int min = minSet.size();

                    mins[varIdx] = Math.min(mins[varIdx], min);


                    Set<Integer> involvedSetSizes = varIdxToInvolvedSetSizes[varIdx];
                    Set<C> maxSet = slice.getLargestValueSetForVarIdx(varIdx);
                    involvedSetSizes.add(min);
                    involvedSetSizes.add(maxSet.size());
                }
            }

            for (SliceNode<?, C> slice : slices) {
                for (int varIdx : slice.getRemainingVars()) {
                    int mmax = slice.getLargestValueSetForVarIdx(varIdx).size();

                    varToScore[varIdx] *= (mins[varIdx] / (float)mmax);
                }
            }

            for (int varIdx : remainingVarIdxs) {
                int numInvolvedSetSizes = varIdxToInvolvedSetSizes[varIdx].size();
                varToScore[varIdx] /= (float)numInvolvedSetSizes;
            }

            float bestVarIdxScore = varToScore[bestVarIdx];
            for (int varIdx : remainingVarIdxs) {
                float score = varToScore[varIdx];

                if (score < bestVarIdxScore) {
                    bestVarIdxScore = score;
                    bestVarIdx = varIdx;
                }
            }

// DEBUG POINT
//            if (false) {
//                System.out.println("stats");
//            }
        }
        return bestVarIdx;
    }

}

//if (nextSlice.getRemainingVars().length == 0) {
//int[] varIdxs = nextSlice.getInitialVarIdxs();
//Object[] values = nextSlice.getVarIdxToSliceValue();
//
//// System.out.println("Solution found!");
//// for (int i = 0; i < varIdxs.length; ++i) {
//// int varIdx = varIdxs[i];
//// Object val = values[i];
//// varIdxToValues.get(varIdx).add((C) val);
////
//// }
//} else {
//allNextSlices.add(nextSlice);
//}
