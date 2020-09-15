package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.atlas.lib.persistent.PSet;
import org.apache.jena.atlas.lib.persistent.PersistentSet;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;


public class TupleQueryAnalyzer {

    /**
     * Simple analysis a tuple query against an index structure.
     * Yields for each path through the index a report about the set of components
     * of the tuple-like that can be used for indexed lookup
     *
     * The analysis does not consider e.g. histograms - however a simple
     * weighting mechanism of components is supported - they weights should roughly proportional to
     * the number of distinct values in each component.
     * E.g. there is a 'many' subjects, 'few' predicates and 'very many' objects could be expressed as
     * [10, 1, 100]
     * This is used as a basis for pair-wise cardinality estimates e.g. s to o would be 10:1000
     *
     * @param <TupleLike>
     * @param <ComponentType>
     * @param node
     * @param tupleQuery
     * @param int[] componentWeights
     * @return
     */
    public static <TupleLike, ComponentType> List<IndexPathReport> analyze(
            TupleQuery<ComponentType> tupleQuery,
            StoreAccessor<TupleLike, ComponentType> node,
            int[] componentWeights) {

        // First we look for candidates that answer the constraints efficiently
        // Then check whether it is possible to also answer the projection

        Map<StoreAccessor<TupleLike, ComponentType>, PersistentSet<Integer>> patternMatches = new IdentityHashMap<>();

        analyzeForPattern(tupleQuery, node, PSet.empty(), patternMatches);

        // If there are suitable index nodes then pick the one deemed to be most selective
        // The component weights are use for this purpose


        // Sort candidates with most number of components matched to keys first
//      Collections.sort(patternMatches,
//              (a, b) -> a.getMatchedComponents().asSet().size() - b.getMatchedComponents().asSet().size());



        // Distinct may become turned off if the projection can be served directly from
        // an index's top level surface form
        boolean applyDistinctOnResult = tupleQuery.isDistinct();


        if (tupleQuery.isDistinct() && tupleQuery.hasProject()) {

            int[] project = tupleQuery.getProject();
            Set<Integer> proj = IntStream.of(project).boxed().collect(Collectors.toSet());

            List<StoreAccessor<TupleLike, ComponentType>> projectionMatches = new ArrayList<>();

            // By 'deepening' the found candidates we may be able to serve remaining components
            // of the requested projection
            for (Entry<StoreAccessor<TupleLike, ComponentType>, PersistentSet<Integer>> candidate : new ArrayList<>(patternMatches.entrySet())) {

                boolean canServeProjection = candidate.getValue().asSet()
                        .containsAll(proj);

                if (canServeProjection) {
                    projectionMatches.add(candidate.getKey());
                } else {
                    // Check whether by deepening the current node to its descendants would cover the projection
                    // Performs depth first pre order search
                    projectionMatches = DepthFirstSearchLib.conditionalDepthFirstInOrderWithParent(
                            candidate.getKey(),
                            null,
                            StoreAccessor::getChildren,
                            (n, parent) -> { // non-reflexive; parent is never null

                                // Beware of the side effects!
                                PersistentSet<Integer> cover = patternMatches.get(parent);
                                PersistentSet<Integer> nextCover = plus(cover, n);
                                patternMatches.put(n, nextCover);

                                boolean canExpansionServeProjection = nextCover.asSet().containsAll(proj);

                                // System.out.println("Expansion for projection: " + n + " can serve " + canExpansionServeProjection + " " + nextCover.asSet() + " requested " + proj);

                                // True indicates successful match and terminates the search on the branch
                                return canExpansionServeProjection;
                            }).collect(Collectors.toList());

                }
            }

            System.out.println("Can serve projection " + proj + " from " + projectionMatches);

        }

        // If no best candidate for the pattern was found we need to scan all tuples anyway
        // For this purpose scan the content of the least-nested leaf node
        if (true) {
            node.leastNestedChildOrSelf();

            // We need the following access mechanisms:
            // (.) stream the key set of some nested map (TODO the key must be tuple like so we can restore components!)
            // (.) get the leaf values at some path (i.e. the quads themselves - skipping any keys in between)
            // (.) stream several surface components of inner maps to create result tuples
            //        this is a cartesian product like operation w.r.t. a given pattern

            //
            // So how to get the data out for surfaces efficiently?
            // (a) create nested tuples such as Tuple.create(parent, newKey, newValue)
            //    This is actually done right now with streamEntries
            //
            // (b) create flat tuples for each stream
            //   create a surface stream up to a certain depth - then
            //   surfaceStream.map(??? -> ???)
            // Or

//            node.buildStream()
//            	.path(leastNestedIndexe)
//            	.

        }

        return new ArrayList<>();
    }



    /**
     * Helper function to add the tuple indices of an index node to an existing persistent set of indices
     *
     * @param <D>
     * @param <C>
     * @param matchedComponents
     * @param node
     * @return
     */
    public static <D, C> PersistentSet<Integer> plus(PersistentSet<Integer> matchedComponents, StoreAccessor<D, C> node) {
        PersistentSet<Integer> result = matchedComponents;
        int[] currentIdxs = node.getStorage().getKeyTupleIdxs();
        for (int i = 0; i < currentIdxs.length; ++i) {
            result = result.plus(currentIdxs[i]);
        }
        return result;
    }



    /**
     * Depth first pre order travesal to find the deepest nodes for which
     * no descendants can match more constraints of the tuple query
     *
     *
     * @param <TupleLike>
     * @param <ComponentType>
     * @param tupleQuery
     * @param node
     * @param matchedComponents
     * @param candidates
     * @return
     */
    public static <TupleLike, ComponentType> boolean analyzeForPattern(
            TupleQuery<ComponentType> tupleQuery,
            StoreAccessor<TupleLike, ComponentType> node,
            PersistentSet<Integer> matchedComponents,
            Map<? super StoreAccessor<TupleLike, ComponentType>, PersistentSet<Integer>> candidates
            ) {

//        PathReport result = new PathReport(parent, altIdx);

        int[] currentIxds = node.getStorage().getKeyTupleIdxs();

        boolean canDoIndexedLookup = true;
        for (int i = 0; i < currentIxds.length; ++i) {
            int componentIdx = currentIxds[i];
            ComponentType c = tupleQuery.getConstraint(componentIdx);
            if (c == null) {
                canDoIndexedLookup = false;
            }
        }

        // Iterate through alternative subindexes whether any is more specific for the pattern than the current match
        boolean foundEvenBetterCandidate = false;

        if (canDoIndexedLookup) {
            PersistentSet<Integer> newMatchedComponents = matchedComponents;
            for (int i = 0; i < currentIxds.length; ++i) {
                newMatchedComponents = newMatchedComponents.plus(currentIxds[i]);
            }

            //IndexPathReport betterCandidate = new IndexPathReport(parent, altIdx, node, newMatchedComponents);

            // If none of the children matches any more components than return this
//            for(Meta2Node<TupleLike, ComponentType, ?> child : node.getChildren()) {
            List<? extends StoreAccessor<TupleLike, ComponentType>> children = node.getChildren();
            for (int childIdx = 0; childIdx < children.size(); ++childIdx) {
                StoreAccessor<TupleLike, ComponentType> child = children.get(childIdx);
                foundEvenBetterCandidate = foundEvenBetterCandidate || analyzeForPattern(
                        tupleQuery,
                        child,
                        newMatchedComponents,
                        candidates);
            }
            if (!foundEvenBetterCandidate) {
                // candidates.add(betterCandidate);
                candidates.put(node, matchedComponents);
            }
        }

        return canDoIndexedLookup && !foundEvenBetterCandidate;
    }
}
