package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.ResultStreamer;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;
import org.apache.jena.ext.com.google.common.collect.ComparisonChain;

import com.github.andrewoma.dexx.collection.LinkedLists;


/**
 * Compare NodeStats instances by selectivty w.r.t. componentWeights
 *
 * Uses a very simple heuristic
 * <ol>
 * <li>Number of matched components</li>
 * <li>Component weight</li>
 * <li>Depth of the index node</li> (e.g. prefers ([SP] ->[]) over ([S] -> ([P] -> [])
 * </li>On tie: node id for determinism</li>
 * </ol>
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 */
class NodeStatsComparator<D, C>
    implements Comparator<NodeStats<D, C>>
{
    protected int[] componentWeights;

    public NodeStatsComparator(int[] componentWeights) {
        super();
        this.componentWeights = componentWeights;
    }

    public static <D, C> NodeStatsComparator<D, C> forWeights(int[] componentWeights) {
        return new NodeStatsComparator<D, C>(componentWeights);
    }

    @Override
    public int compare(NodeStats<D, C> a, NodeStats<D, C> b) {
        int[] matchesA = a.getMatchedComponentsSet().stream().mapToInt(x -> x).toArray();
        int[] matchesB = b.getMatchedComponentsSet().stream().mapToInt(x -> x).toArray();

        int result = ComparisonChain.start()
            .compare(matchesA.length, matchesB.length)
            .compare(matchesA, matchesB, (x, y) -> compareIndirect(x, y, componentWeights))
            .compare(a.getAccessor().depth(), b.getAccessor().depth())
            .compare(a.getAccessor().id(), b.getAccessor().id())
            .result();

        return result;
    }

    public static int compareIndirect(int[] a, int[] b, int[] weights) {
        int result = 0;
        int l = Math.min(a.length, b.length);
        for(int i = 0; i < l; ++i) {
            int itemA = a[i];
            int itemB = b[i];

            int weightA = weights[itemA];
            int weightB = weights[itemB];

            int delta = weightB - weightA;
            if (delta != 0) {
                result = delta;
                break;
            }
        }

        return result;
    }

    /**
     * SPOG
     * [ 1000, 1, 1000000, 10000]
     *
     *
     *
     * @param matchedTupleIdxs
     * @param componentWeights
     */
//    public static selectivityScore(int[] matchedTupleIdxs, int[] componentWeights) {
//        for (int i = 0; i < matchedTupleIdxs.length; ++i) {
//            int tupleIdx = matchedTupleIdxs[i];
//            int weight = componentWeights[i];
//
//
//        }
//    }
}


public class TupleQueryAnalyzer {


//    public static createPlanForTuples(TupleQuery<ComponentType> tupleQuery,
//            StoreAccessor<TupleLike, ComponentType> node,
//            int[] componentWeights) {
//
//
//    }

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


        // The best candidate
        NodeStats<D, C> result = null;

        // First we look for candidates that answer the constraints efficiently
        // Then check whether it is possible to also answer the projection

        List<NodeStats<D, C>> patternMatches = new ArrayList<>();
        analyzeForPattern(tupleQuery, node, LinkedLists.of(), patternMatches);

        if (patternMatches.isEmpty()) {
            throw new RuntimeException("Found 0 nodes in the index structure to answer request; index is empty or internal error");
        }

        NodeStatsComparator<D, C> nodeStatsComparator = NodeStatsComparator.forWeights(componentWeights);

        Collections.sort(patternMatches, nodeStatsComparator);

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

            int[] project = tupleQuery.getProject();
            java.util.Set<Integer> proj = IntStream.of(project).boxed().collect(Collectors.toSet());

            // By 'deepening' the found candidates we may be able to serve remaining components
            // of the requested projection
            for (NodeStats<D, C> candidate : new ArrayList<>(patternMatches)) {

                boolean canServeProjection = candidate.getMatchedComponentsSet()
                        .containsAll(proj);

                if (canServeProjection) {
                    projectionMatches.add(candidate);
                } else {
                    // Check whether by deepening the current node to its descendants would cover the projection
                    // Performs depth first pre order search
                    projectionMatches = DepthFirstSearchLib.conditionalDepthFirstInOrderWithParentAndIndirectChildren(
                            candidate,
                            null,
                            // Indirect access to children
                            stats -> stats.getAccessor().getChildren(),

                            // Construction of succcessor node from indirect child and parent
                            (child, parent) -> { // non-reflexive; parent is never null
                                com.github.andrewoma.dexx.collection.List<Integer> cover = parent.getMatchedComponents();
                                com.github.andrewoma.dexx.collection.List<Integer> nextCover = plus(cover, child);
                                NodeStats<D, C> nextNode = new NodeStats<>(child, nextCover);
                                return nextNode;
                            },

                            // Abort if true
                            (nextNode, p) -> {
                                boolean canExpansionServeProjection = nextNode.getMatchedComponentsSet().containsAll(proj);

                                // Beware of the side effect!!!
                                // We found a better match than the parent so
                                // remove a possible priorly matched parent
                                patternMatches.remove(p);
                                patternMatches.add(nextNode);

                                return canExpansionServeProjection;
                            }).collect(Collectors.toList());
                }

                if (!projectionMatches.isEmpty()) {
                    // Sort the projection matches by depth
                    Collections.sort(projectionMatches, (a, b) -> b.getAccessor().depth() - a.getAccessor().depth());

                    // Pick first match
                    result = projectionMatches.get(0);
                    System.out.println("Can serve projection " + proj + " from " + projectionMatches);
                    break;
                }
            }
        }

        // If no best candidate for the pattern was found we need to scan all tuples anyway
        // For this purpose scan the content of the least-nested leaf node
        if (result == null) {
            NodeStats<D, C> bestMatchForPattern = patternMatches.get(0);
            StoreAccessor<D, C> accessorForContent = bestMatchForPattern.getAccessor().leastNestedChildOrSelf();
            result = new NodeStats<>(accessorForContent, LinkedLists.of());
        }

        return result;
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
     * @param matchedComponents
     * @param candidates
     * @return
     */
    public static <D, C> boolean analyzeForPattern(
            TupleQuery<C> tupleQuery,
            StoreAccessor<D, C> node,
            com.github.andrewoma.dexx.collection.List<Integer> matchedComponents,
            List<NodeStats<D, C>> candidates
            ) {

        int[] currentIxds = node.getStorage().getKeyTupleIdxs();

        boolean canDoIndexedLookup = true;
        for (int i = 0; i < currentIxds.length; ++i) {
            int componentIdx = currentIxds[i];
            C c = tupleQuery.getConstraint(componentIdx);
            if (c == null) {
                canDoIndexedLookup = false;
            }
        }

        // Iterate through alternative subindexes whether any is more specific for the pattern than the current match
        boolean foundEvenBetterCandidate = false;

        if (canDoIndexedLookup) {
            com.github.andrewoma.dexx.collection.List<Integer> newMatchedComponents = matchedComponents;
            for (int i = 0; i < currentIxds.length; ++i) {
                newMatchedComponents = newMatchedComponents.append(currentIxds[i]);
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
            if (!foundEvenBetterCandidate) {
                // candidates.add(betterCandidate);
                NodeStats<D, C> candidate = new NodeStats<>(node, matchedComponents);
                candidates.add(candidate);
            }
        }

        return canDoIndexedLookup && !foundEvenBetterCandidate;
    }


    public static <D, C, X, T> ResultStreamer<D, C, X> createResultStreamer(
            StoreAccessor<D, C> accessor,
            int[] projection,
            T pattern,
            TupleAccessorCore<? super T, ? extends C> patternAccessor
            ) {

//    	TupleQue

        // Assumption: Leaf nodes contain domain objects
        // TODO This code will break if the  assumption is lifted
        if (accessor.getChildren().isEmpty()) {
            Streamer<?, D> contentStream = accessor
                    .streamerForContent(pattern, patternAccessor);

            // Any projection needs to be served from the content

        } else {
            if (projection.length == 1) {
                // Here we assume that the accessor node is positioned on the one the holds the
                // keys we want to project
                Streamer<?, C> componentStream = accessor
                        .streamerForKeys(pattern, patternAccessor, null, KeyReducers.projectOnly(accessor.depth()));
            } else {
                // We need to project tuples
                KeyReducerTuple<C> keyToTupleReducer = KeyReducerTuple.createForProjection(accessor, projection);

                Streamer<?, Tuple<C>> tupleStream = store -> accessor.cartesianProduct(
                        pattern,
                        patternAccessor,
                        //Quad.create(Node.ANY, Node.ANY, Node.ANY, q4.getObject()),
                        keyToTupleReducer.newAccumulator(),
                        keyToTupleReducer)
                .streamRaw(store).map(Entry::getKey).map(keyToTupleReducer::makeTuple);

            }
        }




        return null;
    }

}
