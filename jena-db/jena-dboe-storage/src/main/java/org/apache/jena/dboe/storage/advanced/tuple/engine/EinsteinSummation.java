package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
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

        BiMap<C, Integer> varToVarIdx = HashBiMap.create();
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
            SliceNode<D, C> tupleSlice = SliceNode.create(rootNode, store, remainingVarIdxs, varIdxToTupleIdxs);

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


        System.out.println("Entering recursion");
        recurse(varDim, varIdxs, initialSlices);

        for (int i = 0; i < varDim; ++i) {
            System.out.println("Solution: " + varToVarIdx.inverse().get(i) + ": " + varIdxToValues.get(i).size());
        }

    }

    class RecursionState<D, C> {
        int[] remainingVarIdxs;
        List<SliceNode<D, C>> slices;
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
    public static <D, C> void recurse(
            int varDim,
            int[] remainingVarIdxs,
            List<SliceNode<D, C>> slices) {

        boolean debug = true;

//        if (debug) System.out.println("Recursion started: RemainingVarIdxs: " + Arrays.toString(remainingVarIdxs) + " numSlices=" + slices.size());
//        if (slices == null || slices.isEmpty()) {
//            return false;
//        }
//
        if (remainingVarIdxs.length == 0) {
            return;
        }

        // Find out which variable to pick
        // For each varIdx iterate all slices and find out the minimum and maximum number of valuee


        int pickedVarIdx = findBestSliceVarIdx(varDim, remainingVarIdxs, slices);

        int[] nextRemainingVarIdxs = ArrayUtils.removeElement(remainingVarIdxs, pickedVarIdx);
//        if (debug) System.out.println("Picked var " + pickedVarIdx + " among " + Arrays.toString(remainingVarIdxs) + " with now remaining " + Arrays.toString(nextRemainingVarIdxs));

        // Find all slices that project that variable in any of its remaining components
        // Use an identity hash set in case some of the sets turn out to be references to the same set
        Set<Set<C>> valuesForPickedVarIdx = Sets.newIdentityHashSet();
        for (SliceNode<D, C> slice : slices) {
            int[] varInSliceComponents = slice.getComponentsForVar(pickedVarIdx);

            if (varInSliceComponents != null) {
                for(int tupleIdx : varInSliceComponents) {
                    Set<C> valuesContrib = slice.getValuesForComponent(tupleIdx); //pickedVarIdx);

                    if (valuesContrib.isEmpty()) {
                        System.out.println("should this happen?");
                    }

                    valuesForPickedVarIdx.add(valuesContrib);
                }
            }
        }


        // Created the intersection of all value sets
        // Sort the contributions by size (smallest first)
        List<Set<C>> valueContribs = new ArrayList<>(valuesForPickedVarIdx);

        // The sorting of the sets by size is highly important!
        // Consider computing the intersection between two sets of sizes 100.000 and 1
        // We do not want to copy 100K values just to retain 1
        Collections.sort(valueContribs, (a, b) -> a.size() - b.size());

//        System.out.println("Value set sizes to intrsect: " + valueContribs.stream().map(x -> "" + x.size()).collect(Collectors.joining(", ")));

        Set<C> remainingValuesOfPickedVar = Collections.emptySet();

        if (valueContribs.size() == 1) {
            remainingValuesOfPickedVar = valueContribs.get(0);
        } else if (valueContribs.size() > 1) {
            remainingValuesOfPickedVar = new HashSet<>(valueContribs.get(0));
            for (int i = 1; i < valueContribs.size(); ++i) {
                Set<C> contrib = valueContribs.get(i);
                remainingValuesOfPickedVar.retainAll(contrib);
            }
        }

        // Set<C> remainingValuesOfPickedVarTmp =
        // valueContribs.stream().reduce(Sets::intersection).orElse(Collections.emptySet());
//        System.out.println("Intersection size: " + remainingValuesOfPickedVarTmp.size());
//        if (!remainingValuesOfPickedVarTmp.isEmpty()) {
//
        recurseWork(varDim, nextRemainingVarIdxs, slices, pickedVarIdx,
                remainingValuesOfPickedVar);


    }

    public static <C, D> void recurseWork(
            int varDim,
            int[] nextRemainingVarIdxs,
            List<SliceNode<D, C>> slices,
            int pickedVarIdx,
            Set<C> remainingValuesOfPickedVar) {
        List<SliceNode<D, C>> sliceableByPickedVar = new ArrayList<>();
        List<SliceNode<D, C>> nonSliceableByPickedVar = new ArrayList<>();

        for (SliceNode<D, C> slice : slices) {
            if (slice.hasRemainingVarIdx(pickedVarIdx)) {
                sliceableByPickedVar.add(slice);
            } else {
                nonSliceableByPickedVar.add(slice);
            }
        }

        // Materialize the intersection
//            Set<C> remainingValuesOfPickedVar = new HashSet<>(remainingValuesOfPickedVarTmp);
//            System.out.println("Next slices for var " + pickedVarIdx + ": " + nextSlices.size());
//        if (debug) System.out.println("Need to probe " + remainingValuesOfPickedVar.size() + " values; pickedeVarIdx = " + pickedVarIdx);

        // List<SliceNode<D, C>> allNextSlices = new
        // ArrayList<>(nonSliceableByPickedVar); // new ArrayList<>();
//            List<SliceNode<D, C>> allNextSlices = new ArrayList<>();

        int valueCounter = 0;
        for (C value : remainingValuesOfPickedVar) {
//        System.out.println("Processing value " + ++valueCounter + "/" + remainingValuesOfPickedVar.size());
//                if (debug) System.out.println("Probing picked var " + pickedVarIdx + " with " + value);

            List<SliceNode<D, C>> allNextSlices = new ArrayList<>(nonSliceableByPickedVar);

            for (SliceNode<D, C> slice : sliceableByPickedVar) {
//                if (slice.hasRemainingVarIdx(pickedVarIdx)) {
                SliceNode<D, C> nextSlice = slice.sliceOnVarIdxAndValue(pickedVarIdx, value);

                if (nextSlice != null) {

                    if (nextSlice.getRemainingVars().length == 0) {
                        int[] varIdxs = nextSlice.getInitialVarIdxs();
                        Object[] values = nextSlice.getVarIdxToSliceValue();

//                        System.out.println("Solution found!");
//                        for (int i = 0; i < varIdxs.length; ++i) {
//                            int varIdx = varIdxs[i];
//                            Object val = values[i];
//                            varIdxToValues.get(varIdx).add((C) val);
//
//                        }
                    } else {
                        allNextSlices.add(nextSlice);
                    }
                }
//                } else {
//                    allNextSlices.add(slice);
//                }

            }

            // All slices that mentioned a certain var are now constrained to one of the var's value
            recurse(varDim, nextRemainingVarIdxs, allNextSlices);
        }
    }

    public static <D, C>  int findBestSliceVarIdx(int varDim, int[] remainingVarIdxs, List<SliceNode<D, C>> slices) {
        // The score is a reduction factor - the higher the better
        int bestVarIdx = remainingVarIdxs[0];

        if (remainingVarIdxs.length > 1) {
//            int varDim = varIdxToValues.size();
            int[] mins = new int[varDim];
            // int[] maxs = new int[varDim];

            float[] varToScore = new float[varDim];

            Arrays.fill(mins, Integer.MAX_VALUE);
            Arrays.fill(varToScore, 1.0f);
//            Arrays.fill(maxs, 0);

            for (SliceNode<D, C> slice : slices) {
                for (int varIdx : slice.getRemainingVars()) {
                    int min = slice.getSmallestValueSetForVarIdx(varIdx).size();
                    //int max = slice.getLargestValueSetForVarIdx(varIdx).size();

                    mins[varIdx] = Math.min(mins[varIdx], min);
                    // maxs[varIdx] = Math.max(maxs[varIdx], max);
                }
            }

            for (SliceNode<D, C> slice : slices) {
                for (int varIdx : slice.getRemainingVars()) {
                    int mmax = slice.getLargestValueSetForVarIdx(varIdx).size();

                    varToScore[varIdx] *= mmax / (float)mins[varIdx];
                }
            }


            float bestVarIdxScore = 0f;
            for (int varIdx : remainingVarIdxs) {
                float score = varToScore[varIdx];

                if (score > bestVarIdxScore) {
                    bestVarIdxScore = score;
                    bestVarIdx = varIdx;
                }
            }

//            if (false) {
//                System.out.println("stats");
//            }
        }
        return bestVarIdx;
    }

}




// recurse(nextRemainingVarIdxs, nonSliceableByPickedVar, varIdxToValues);


//nonSliceableByPickedVar.addAll(next)


//boolean subResult = recurse(nextRemainingVarIdxs, nonSliceableByPickedVar, varIdxToValues);
//boolean subResult = recurse(nextRemainingVarIdxs, allNextSlices, varIdxToValues);

//result = result || subResult;

//boolean subResult = recurse(nextRemainingVarIdxs, allNextSlices, varIdxToValues);
//if (subResult) {
//    varIdxToValues.get(pickedVarIdx).add(value);
//}

//result = result || subResult;


//List<SliceNode<D, C>> nextSlices = sliceableByPickedVar.stream()
//  .map(slice -> {
//      SliceNode<D, C> r = slice.sliceOnVarIdxAndValue(pickedVarIdx, value);
////      SliceNode<D, C> r = slice.remainingVars.length == 0
////              ? null
////              : slice.sliceOnVarIdxAndValue(pickedVarIdx, value);
//      return r;
//  })
//  // There is no flatMap with nulls; creating intermediate streams seems like a waste
//  .filter(newSlice -> newSlice != null)
//  .collect(Collectors.toList());
//
//allNextSlices.addAll(nextSlices);
//System.out.println("Next slices for var " + pickedVarIdx + ": " + nextSlices.size());

//boolean subResult = recurse(nextRemainingVarIdxs, nextSlices, varIdxToValues);
//if (subResult) {
//  varIdxToValues.get(pickedVarIdx).add(value);
//}
//
//result = result || subResult;

//}
