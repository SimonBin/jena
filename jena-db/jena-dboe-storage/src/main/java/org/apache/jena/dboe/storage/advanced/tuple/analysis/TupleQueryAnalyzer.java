package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.atlas.lib.persistent.PersistentSet;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;

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
     *
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
            Meta2Node<TupleLike, ComponentType, ?> node,
            int[] componentWeights) {

        List<IndexPathReport> patternMatches = new ArrayList<>();

        // Sort candidates with most number of components matched to keys first
        Collections.sort(patternMatches,
                (a, b) -> a.getMatchedComponents().asSet().size() - b.getMatchedComponents().asSet().size());

        IndexPathReport root = new IndexPathReport(null, -1);

        analyzeForPattern(tupleQuery, node, root, 0, patternMatches);

        // If there are no matching candidates pick any index among those with the lowest nested depth
        // so that we need to perform the least number of nested lookups

//        List<Integer> pathToLeastNestedNode = null;
//
//        if (candidates.isEmpty()) {
//            pathToLeastNestedNode =  Meta2NodeLib.findLeastNestedIndexNode(node);
//
//        }


        // First we look for candidates that answer the constraints efficiently
        // For this we want as many components correspond to keys in the index as possible

        // If the request is non-distinct we have two options:
        // (1) just iterate and project the quads
        // (2) use the index and emit the surface according to some cardinalities
        // We don't track cardinalities (yet) so go with (1)

        if (tupleQuery.isDistinct() && tupleQuery.hasProject()) {

            int[] project = tupleQuery.getProject();
            Set<Integer> proj = IntStream.of(project).boxed().collect(Collectors.toSet());

            List<IndexPathReport> projectionMatches = new ArrayList<>();

            // TODO We may expand a candidate
            for (IndexPathReport candidate : patternMatches) {

                boolean canServeProjection = candidate.getMatchedComponents().asSet()
                        .containsAll(proj);

                if (canServeProjection) {
                    projectionMatches.add(candidate);
                }
//            	while (start != null) {
//            		int[] start.getIndexNode().getKeyTupleIdxs();
//            	}
            }

            System.out.println("Can serve projection " + proj + " from " + projectionMatches);
        }

        // If no best candidate for the pattern was found we need to scan all tuples anyway
        if (true) {
            List<Integer> leastNestedIndex =  Meta2NodeLib.findLeastNestedIndexNode(node);

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

        return patternMatches;
    }

//    public static <N> Stream<com.github.andrewoma.dexx.collection.List<N>>
//    breadthFirstPaths2(N rootItem, Function<? super N, ? extends Collection<? extends N>> successorFn) {
//        return breadthFirstPaths(LinkedLists.of(rootItem), parent -> successorFn.apply(parent).stream().map(x -> (N)x));
//    }




    public static <TupleLike, ComponentType> boolean analyzeForPattern(
            TupleQuery<ComponentType> tupleQuery,
            Meta2Node<TupleLike, ComponentType, ?> node,
            IndexPathReport parent, // can be used recursion depth
            int altIdx,
            Collection<IndexPathReport> candidates
            ) {

//        PathReport result = new PathReport(parent, altIdx);

        int[] currentIxds = node.getKeyTupleIdxs();

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
            PersistentSet<Integer> newMatchedComponents = parent.getMatchedComponents();
            for (int i = 0; i < currentIxds.length; ++i) {
                newMatchedComponents = newMatchedComponents.plus(currentIxds[i]);
            }

            IndexPathReport betterCandidate = new IndexPathReport(parent, altIdx, node, newMatchedComponents);

            // If none of the children matches any more components than return this
//            for(Meta2Node<TupleLike, ComponentType, ?> child : node.getChildren()) {
            List<? extends Meta2Node<TupleLike, ComponentType, ?>> children = node.getChildren();
            for (int childIdx = 0; childIdx < children.size(); ++childIdx) {
                Meta2Node<TupleLike, ComponentType, ?> child = children.get(childIdx);
                foundEvenBetterCandidate = foundEvenBetterCandidate || analyzeForPattern(
                        tupleQuery,
                        child,
                        betterCandidate,
                        childIdx,
                        candidates);
            }
            if (!foundEvenBetterCandidate) {
                candidates.add(betterCandidate);
            }
        }

        return canDoIndexedLookup || foundEvenBetterCandidate;
    }
}
