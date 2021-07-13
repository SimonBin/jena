package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.BiReducer;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.IndexedKeyReducer;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNode;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;


public class EinsteinSummation {

    public static final Predicate<int[]> TRUE = any -> true;

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
            boolean distinct,
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
                distinct,
                projectVars,
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
            boolean distinct,
            Set<? extends C0> requestProjection,
            A initialAccumulator,
            BiReducer<A, C0, C0> reducer // first C = variable, second C = value; both as Nodes
            )
    {
        int tupleDim = tupleAccessor.getDimension();

        // First find out the set of variables
        // Essential vars are those with more than 1 mention
        HashMap<C0, Integer> mentionedVars = new LinkedHashMap<>();

        for (T tuple : btp) {
            for (int i = 0; i < tupleDim; ++i) {
                C0 c = tupleAccessor.get(tuple, i);
                if (isVar.test(c)) {
                    mentionedVars.compute(c, (k, v) -> v == null ? 1 : v + 1);
                }
            }
        }

        Set<C0> distinguishedVars;
        Set<C0> nonDistinguishedVars;

        if (requestProjection == null) {
            distinguishedVars = mentionedVars.keySet();
            nonDistinguishedVars = Collections.emptySet();
        } else {
            Set<C0> validProjection = new LinkedHashSet<>();
            validProjection.addAll(requestProjection);
            validProjection.retainAll(mentionedVars.keySet());

            distinguishedVars = validProjection;

            if (distinct) {
                // Essential variables
                // Definition Attempt 1: Non-joining non-distinguished variables
                //     (co-occurring on the same triple pattern as distinguished variables)
                //     can be omitted ignored
                //  ^ The restriction to co-occurrence is probably not needed:
                //  Either there is a triple pattern with a joining variable - then we need to compute the join
                //  Or, consider the following:
                //

                // SELECT DISTINCT ?p { ?s ?p ?o } -> ?s and ?o are non-essential
                // SELECT DISTINCT ?p { ?s ?p ?o . ?x ?y ?z } -> ?s and ?o are non-essential
                //     But actually ?x ?y ?z is non-essential because if there is a solution at all
                //     then this triple pattern will have solutions as well
                //     If instead of ?y there was constant { ?x :c ?z }, then we check whether constant
                //     is in the index first anyway, which makes ?x and ?z non-essential

//                Set<C0> essentialVars = mentionedVars.keySet();
                Set<C0> essentialVars = mentionedVars.entrySet().stream()
                        .filter(e -> e.getValue() > 1).map(Entry::getKey).collect(Collectors.toSet());

                nonDistinguishedVars = Sets.difference(essentialVars, distinguishedVars);
            } else {
                nonDistinguishedVars = Sets.difference(mentionedVars.keySet(), distinguishedVars);
            }
        }

//        System.out.println("Btp vars for " + btp);
//        System.out.println("  Distinct " + distinct);
//        System.out.println("  Requested " + requestProjection);
//        System.out.println("  Mentioned " + mentionedVars);
//        System.out.println("  Distinguished " + distinguishedVars);
//        System.out.println("  Non-Distinguished " + nonDistinguishedVars);

        int distinguishedVarSize = distinguishedVars.size();

        // set up a vector of variable indices

        // Put the outvars first in the list and the internal ones after
        // When invoking the accumulator we know whether a var is projected if it is at the start of the list
        // [ outvar1..k  internalvark+1..n]
        int varDim = distinguishedVars.size() + nonDistinguishedVars.size();
        List<C0> varList = new ArrayList<>(varDim);
        varList.addAll(distinguishedVars);
        varList.addAll(nonDistinguishedVars);

        int[] varIdxs = new int[varDim];

        // Use var = varList[varIdx] for the reverse mapping
        Map<C0, Integer> varToProjVarIdx = new HashMap<>();
        for (int r = 0; r < varDim; ++r) {
            varToProjVarIdx.put(varList.get(r), r);
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
                Integer varIdx = varToProjVarIdx.get(rawValue);

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
                if (!isVar.test(rawValue)) {
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


        // Wrap the incoming reducer with another one that maps var indices to the actual vars
        IndexedKeyReducer<A, C> varIdxBasedReducer = (acc, varIdx, encodedValue) -> {
            A r;
            // Distinguished variables have lower indices
            // For non-distinguished variables just pass on the accumulator
            if (varIdx < distinguishedVarSize) {
                C0 var = varList.get(varIdx);
                C0 decodedValue = componentDecoder.apply(encodedValue);
                r = reducer.reduce(acc, var, decodedValue);
            } else {
                r = acc;
            }
            return r;
        };

        // If distinct is enabled then we do not have to iterate
        // all combinations of non-distinguished variables:
        // E.g. if we have SELECT DISTINCT ?x ?y ?z { ?x ?y ?z . ?a ?b ?c }
        // then as soon as ?x ?y ?z are no longer remaining we can abort after a single
        // match for ?a ?b ?c
        Predicate<int[]> testAbortOnMatch = !distinct
                ? remainingVarIdxs -> false
                : remainingVarIdxs -> {
                    boolean r = true;
                    for (int i = 0; i < remainingVarIdxs.length; ++i) {
                        int varIdx = remainingVarIdxs[i];

                        // If there is a distinguished variable we cannot abort early
                        if (varIdx < distinguishedVarSize) {
                            r = false;
                            break;
                        }
                    }
                    return r;
                };

        // Do not execute anything unless an operation is invoked on the stream
        Stream<A> stream = Stream.of(initialAccumulator).flatMap(acc -> recurse(
                varDim,
                varIdxs,
                false,
                testAbortOnMatch,
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
            boolean abortOnMatch,
            Predicate<int[]> testAbortOnMatch, // A predicate that can become true if the remaining varIdxs do not contain any distinguished variable
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

        List<SliceNode<?, C>> sliceableByPickedVar;
        List<SliceNode<?, C>> nonSliceableByPickedVar;
        if (slices.size() != 1) {
            sliceableByPickedVar = new ArrayList<>();
            nonSliceableByPickedVar = new ArrayList<>();

            for (SliceNode<?, C> slice : slices) {
                if (slice.hasRemainingVarIdx(pickedVarIdx)) {
                    sliceableByPickedVar.add(slice);
                } else {
                    nonSliceableByPickedVar.add(slice);
                }
            }
        } else {
            SliceNode<?, C> slice = slices.get(0);
            if (slice.hasRemainingVarIdx(pickedVarIdx)) {
                sliceableByPickedVar = Collections.singletonList(slice);
                nonSliceableByPickedVar = Collections.emptyList();
            } else {
                sliceableByPickedVar = Collections.emptyList();
                nonSliceableByPickedVar = Collections.singletonList(slice);
            }

        }

        // Test whether to set the flag that the next iteration should abort after the first match
        // If this call already has the flag set there is no need to recheck it
        boolean abortNextRecursionOnMatch = abortOnMatch || testAbortOnMatch.test(nextRemainingVarIdxs);

        Stream<A> tmpStream = remainingValuesOfPickedVar.stream().flatMap(value -> {
            // Perhaps Interables.concat?
            List<SliceNode<?, C>> allNextSlices = new ArrayList<>(nonSliceableByPickedVar);

            for (SliceNode<?, C> slice : sliceableByPickedVar) {
                SliceNode<?, C> nextSlice = slice.sliceOnVarIdxAndValue(pickedVarIdx, value);

                if (nextSlice != null) {
                    if (nextSlice.getRemainingVars().length != 0) {
                        allNextSlices.add(nextSlice);
                    }
                }

            }
            A nextAccumulator = reducer.reduce(accumulator, pickedVarIdx, value);

            // All slices that mentioned a certain var are now constrained to one of the
            // var's value
            return recurse(varDim, nextRemainingVarIdxs, abortNextRecursionOnMatch, testAbortOnMatch, allNextSlices, nextAccumulator, reducer);
        });

//        Stream<A> result = tmpStream;
        Stream<A> result = abortOnMatch
                ? tmpStream.limit(1)
                : tmpStream;

        return result;
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
