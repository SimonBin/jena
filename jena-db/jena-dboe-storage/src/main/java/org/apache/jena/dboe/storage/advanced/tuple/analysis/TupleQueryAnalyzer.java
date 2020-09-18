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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.TupleOps;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerBinder;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerFromComponent;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerFromDomain;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerFromTuple;

import com.github.andrewoma.dexx.collection.LinkedLists;
import com.github.jsonldjava.shaded.com.google.common.collect.Sets;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TupleQueryAnalyzer {


//    public static createPlanForTuples(TupleQuery<ComponentType> tupleQuery,
//            StoreAccessor<TupleLike, ComponentType> node,
//            int[] componentWeights) {
//
//
//    }

    public static <D, C> NodeStats<D, C> analyze(
            TupleQuery<C> tupleQuery,
            StoreAccessor<D, C> node) {
        return analyze(tupleQuery, node, new int[] {10, 10, 1, 100});
    }

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
     * @param <D>
     * @param <C>
     * @param node
     * @param tupleQuery
     * @param int[] componentWeights
     * @return
     */
    public static <D, C> NodeStats<D, C> analyze(
            TupleQuery<C> tupleQuery,
            StoreAccessor<D, C> node,
            int[] componentWeights) {

        int[] project = tupleQuery.getProject();

        java.util.Set<Integer> proj = project == null
                ? IntStream.range(0, tupleQuery.getDimension()).boxed().collect(Collectors.toCollection(LinkedHashSet::new))
                : IntStream.of(project).boxed().collect(Collectors.toCollection(LinkedHashSet::new));


        // The best candidate
        NodeStats<D, C> result = null;

        // First we look for candidates that answer the constraints efficiently
        // Then check whether it is possible to also answer the projection

        List<NodeStats<D, C>> patternMatches = new ArrayList<>();
        analyzeForPattern(tupleQuery, node, LinkedLists.of(), patternMatches);

        // DEBUG OUTPUT
//        System.err.println("Candidate indexes for " + tupleQuery);
        int l = patternMatches.size();
        for (int i = 0; i < l; ++i) {
            NodeStats<D, C> cand = patternMatches.get(i);
//            System.err.println("Cand " + (i + 1) + "/" + l + ": " + cand);
        }

        if (patternMatches.isEmpty()) {
//             throw new RuntimeException("Found 0 nodes in the index structure to answer request; index is empty or internal error");
            patternMatches.add(new NodeStats<D, C>(node, LinkedLists.of(), LinkedLists.of()));
        }

        NodeStatsComparator<D, C> nodeStatsComparator = NodeStatsComparator.forWeights(componentWeights);

        Collections.sort(patternMatches, nodeStatsComparator);

        // DEBUG OUTPUT
//        System.err.println("Chose candidate: " + patternMatches.get(0));

        // If there are suitable index nodes then pick the one deemed to be most selective
        // The component weights are use for this purpose


        // Sort candidates with most number of components matched to keys first
//      Collections.sort(patternMatches,
//              (a, b) -> a.getMatchedComponents().asSet().size() - b.getMatchedComponents().asSet().size());



        // Distinct may become turned off if the projection can be served directly from
        // an index's top level surface form
        boolean applyDistinctOnResult = tupleQuery.isDistinct();


        List<NodeStats<D, C>> projectionMatches = new ArrayList<>();

        if (tupleQuery.isDistinct() && tupleQuery.hasProject()) {

            // By 'deepening' the found candidates we may be able to serve remaining components
            // of the requested projection

            // Only for the best match
            for (NodeStats<D, C> candidate : Collections.singleton(patternMatches.get(0))) {

                boolean canServeProjection = candidate.getMatchedProjectIdxSet()
                        .containsAll(proj);

                if (canServeProjection) {
                    projectionMatches.add(candidate);
                } else {
                    // Check whether by deepening the current node to its descendants would cover the projection
                    // Performs breadth first search and stops and the first candidate
                    // Note that if for a request of [S] the candidate [S P] comes before [S] then this one is used
                    NodeStats<D, C> betterCandidate = BreadthFirstSearchLib.breadthFirstFindFirstIndirect(
                            candidate,
                            // Indirect access to children
                            stats -> stats.getAccessor().getChildren(),

                            // Construction of successor node from indirect child and parent
                            (rawChild, parent) -> { // non-reflexive; parent is never null
                                com.github.andrewoma.dexx.collection.List<Integer> cover = parent.getMatchedProjectIdxs();
                                com.github.andrewoma.dexx.collection.List<Integer> nextCover = plus(cover, rawChild);

                                NodeStats<D, C> nextNode = new NodeStats<>(rawChild, parent.getMatchedConstraintIdxs(), nextCover);
                                return nextNode;
                            },

                            // Abort if true
                            (child, parent) -> {
                                boolean canExpansionServeProjection =
                                        child.getMatchedProjectIdxSet().containsAll(proj);

                                return canExpansionServeProjection;
                            });

                    if (betterCandidate != null) {
                        projectionMatches.remove(candidate);
                        projectionMatches.add(betterCandidate);
                    }
                }

                if (!projectionMatches.isEmpty()) {
                    // Sort the projection matches by depth
                    Collections.sort(projectionMatches, (a, b) -> b.getAccessor().depth() - a.getAccessor().depth());

                    // Pick first match
                    result = projectionMatches.get(0);

                    // DEBUG OUTPUT
//                    System.err.println("Can serve projection " + proj + " from " + projectionMatches);
                    break;
                }
            }
        }

        // If no best candidate for the pattern was found we need to scan all tuples anyway
        // For this purpose scan the content of the least-nested leaf node
        if (result == null) {
            // FIXME Here we just claim the the projection could be fully served
            // Perhaps an API on the accessor should provide information about which components it serves
            // so that we can easily do clean BFS to the first one that matches on all components

            NodeStats<D, C> bestMatchForPattern = patternMatches.get(0);
            StoreAccessor<D, C> accessorForContent = bestMatchForPattern.getAccessor().leastNestedChildOrSelf();

            result = new NodeStats<>(
                    accessorForContent,
                    bestMatchForPattern.getMatchedConstraintIdxs(),
                    LinkedLists.copyOf(proj));
        }

        return result;
    }



    /**
     * Helper function to succinctly add the tuple indices of an index node
     * to an existing persistent list of indices
     *
     * @param <D>
     * @param <C>
     * @param matchedComponents
     * @param node
     * @return
     */
    public static <D, C> com.github.andrewoma.dexx.collection.List<Integer> plus(com.github.andrewoma.dexx.collection.List<Integer> matchedComponents, StoreAccessor<D, C> node) {
        com.github.andrewoma.dexx.collection.List<Integer> result = matchedComponents;
        int[] currentIdxs = node.getStorage().getKeyTupleIdxs();
        for (int i = 0; i < currentIdxs.length; ++i) {
            result = result.append(currentIdxs[i]);
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
     * @param matchedConstraintIdxs
     * @param candidates
     * @return
     */
    public static <D, C> boolean analyzeForPattern(
            TupleQuery<C> tupleQuery,
            StoreAccessor<D, C> node,
            com.github.andrewoma.dexx.collection.List<Integer> matchedConstraintIdxs,
            List<NodeStats<D, C>> candidates
            ) {

        int[] currentIxds = node.getStorage().getKeyTupleIdxs();

        boolean suitableForIndexLookup = currentIxds.length > 0;
        boolean canDoIndexedLookup = true;

        // Check that we are not facing redundant indexing by the same components
        boolean contributesToCover = false;
        for (int i = 0; i < currentIxds.length; ++i) {
            int componentIdx = currentIxds[i];
            C c = tupleQuery.getConstraint(componentIdx);
            if (c == null) {
                canDoIndexedLookup = false;
            } else {
                contributesToCover = contributesToCover || !matchedConstraintIdxs.asList().contains(componentIdx);
            }
        }

        // Iterate through alternative subindexes whether any is more specific for the pattern than the current match
        boolean foundEvenBetterCandidate = false;

        com.github.andrewoma.dexx.collection.List<Integer> newMatchedComponents = matchedConstraintIdxs;
        if (canDoIndexedLookup) {
            for (int i = 0; i < currentIxds.length; ++i) {
                newMatchedComponents = newMatchedComponents.append(currentIxds[i]);
            }
        }

        // If none of the children matches any more components than return this
        List<? extends StoreAccessor<D, C>> children = node.getChildren();
        for (int childIdx = 0; childIdx < children.size(); ++childIdx) {
            StoreAccessor<D, C> child = children.get(childIdx);
            foundEvenBetterCandidate = foundEvenBetterCandidate || analyzeForPattern(
                    tupleQuery,
                    child,
                    newMatchedComponents,
                    candidates);
        }

        boolean result = contributesToCover && !foundEvenBetterCandidate && canDoIndexedLookup && suitableForIndexLookup;

        if (result) {
            // candidates.add(betterCandidate);
            NodeStats<D, C> candidate = new NodeStats<>(node, newMatchedComponents, newMatchedComponents);
            candidates.add(candidate);
        }

        return result;
    }



    /**
     * Checks whether a domain tuple's components match those of a given pattern tuple.
     * Both tuples are assumed to have the same dimension
     *
     * @param <D>
     * @param <C>
     * @param <P>
     * @param domainItem
     * @param domainAccessor
     * @param recheckIdxs
     * @param pattern
     * @param patternAccessor
     * @return
     */
    public static <D, C, P> boolean recheckCondition(
            D domainItem,
            TupleAccessorCore<D, C> domainAccessor,
            int[] recheckIdxs,
            P pattern,
            TupleAccessorCore<P, C> patternAccessor) {
        boolean result = true;
        for (int i = 0; result && i < recheckIdxs.length; ++i) {
            int tupleIdx = recheckIdxs[i];
            C actual = domainAccessor.get(domainItem, tupleIdx);
            C expected = patternAccessor.get(pattern, tupleIdx);

            result = Objects.equals(expected, actual);
        }

        return result;
    }


    public static <D, C, P> StreamTransform<D, D> transformRecheckCondition(
            TupleAccessorCore<D, C> domainAccessor,
            int[] recheckIdxs,
            P pattern,
            TupleAccessorCore<P, C> patternAccessor) {
        return inStream -> inStream.filter(domainItem -> recheckCondition(domainItem, domainAccessor, recheckIdxs, pattern, patternAccessor));
    }
    /**
     * Creates a streamer for the results of a tuple query.
     * The accessor must be a suitable candidate for answering the query!
     * A simple way to obtain one is with {@link #analyze(TupleQuery, StoreAccessor, int[])}
     *
     *
     * @param <D>
     * @param <C>
     * @param <X>
     * @param <T>
     * @param accessor
     * @param distinct
     * @param projection
     * @param pattern
     * @param patternAccessor
     * @return
     */
    public static <D, C, T> ResultStreamerBinder<D, C, Tuple<C>> createResultStreamer(
            NodeStats<D, C> stats,
            TupleQuery<C> tupleQuery,
            TupleAccessor<D, C> domainAccessor
            ) {


        ResultStreamerBinder<D, C, Tuple<C>> result;


        List<C> pattern = tupleQuery.getPattern();
        int[] projection = tupleQuery.getProject();
        boolean isDistinct = tupleQuery.isDistinct();

        StoreAccessor<D, C> accessor = stats.getAccessor();

        // Find out for which components we need to recheck the filter condition (if any)

        Set<Integer> constrainedComponents = tupleQuery.getConstrainedComponents();
        Set<Integer> indexedComponents = stats.getMatchedConstraintIdxSet();

        Set<Integer> recheckComponents = Sets.difference(constrainedComponents, indexedComponents);
        int[] recheckIdxs = recheckComponents.stream().mapToInt(i -> i).toArray();
//        Set<Integer> constrainedComponents = new LinkedHashSet<>();


        // Assumption: Leaf nodes contain domain objects
        // TODO This code will break if the  assumption is lifted

        // We are returning domain objects so uniqueness is assumeds
        if (accessor.getChildren().isEmpty()) {
            Streamer<?, D> contentStreamer = accessor
                    .streamerForContent(tupleQuery.getPattern(), List::get);

            if (recheckIdxs.length != 0) {
                Streamer<?, D> tmp = contentStreamer;
                contentStreamer = store -> tmp.streamRaw(store)
                        .filter(domainItem -> recheckCondition(
                                domainItem,
                                domainAccessor,
                                recheckIdxs,
                                pattern,
                                List::get));
            }

            // Any projection needs to be served from the content
            // TODO Check if all components are projected
            if (projection != null) {
                Function<D, Tuple<C>> projector = TupleOps.createProjector(projection, domainAccessor);
                Streamer<?, D> tmp = contentStreamer;
                Streamer<?, Tuple<C>> tupleStreamer = store -> tmp.streamRaw(store)
                        .map(projector::apply);

                result = store -> new ResultStreamerFromTuple<D, C>(projection.length, () -> tupleStreamer.streamRaw(store), domainAccessor);
            } else {
                Streamer<?, D> tmp = contentStreamer;
                result = store -> new ResultStreamerFromDomain<D, C>(() -> tmp.streamRaw(store), domainAccessor);
            }

        } else {
            if (projection.length == 1) {
                // Here we assume that the accessor node is positioned on the one the holds the
                // keys we want to project
                Streamer<?, C> componentStreamer = accessor
                        .streamerForKeys(pattern, List::get, null, KeyReducers.projectOnly(accessor.depth()));

                result = store -> new ResultStreamerFromComponent<D, C>(() -> componentStreamer.streamRaw(store), domainAccessor);

            } else {
                // We need to project tuples
                KeyReducerTuple<C> keyToTupleReducer = KeyReducerTuple.createForProjection(accessor, projection);

                Streamer<?, Tuple<C>> tupleStreamer = store -> accessor.cartesianProduct(
                        pattern,
                        List::get,
                        keyToTupleReducer.newAccumulator(),
                        keyToTupleReducer)
                .streamRaw(store).map(Entry::getKey).map(keyToTupleReducer::makeTuple);

                result = store -> new ResultStreamerFromTuple<D, C>(projection.length, () -> tupleStreamer.streamRaw(store), domainAccessor);
            }
        }


        return result;
    }

}
