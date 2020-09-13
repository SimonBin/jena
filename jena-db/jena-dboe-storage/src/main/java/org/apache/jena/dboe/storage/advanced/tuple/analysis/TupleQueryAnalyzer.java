package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jena.atlas.lib.persistent.PersistentSet;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;

public class TupleQueryAnalyzer {

    /**
     * Analyze a tuple query against an index structure.
     * Yields for each path through the index an a report
     *
     *
     *
     * @param <TupleLike>
     * @param <ComponentType>
     * @param node
     * @param tupleQuery
     * @return
     */
    public static <TupleLike, ComponentType> List<PathReport> analyze(
            TupleQuery<ComponentType> tupleQuery,
            Meta2Node<TupleLike, ComponentType, ?> node) {

        List<PathReport> candidates = new ArrayList<>();

        PathReport root = new PathReport(null, -1);

        analyzeForPattern(tupleQuery, node, root, 0, candidates);

        // First we look for candidates that answer the constraints efficiently
        // For this we want as many components correspond to keys in the index as possible

        // The we filter those that can answer the projection most efficiently
        // For this we want the depth as shallow as possible



        return candidates;
    }


    public static <TupleLike, ComponentType> boolean analyzeForPattern(
            TupleQuery<ComponentType> tupleQuery,
            Meta2Node<TupleLike, ComponentType, ?> node,
            PathReport parent, // can be used recursion depth
            int altIdx,
            Collection<PathReport> candidates
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

            PathReport betterCandidate = new PathReport(parent, altIdx, newMatchedComponents);

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
