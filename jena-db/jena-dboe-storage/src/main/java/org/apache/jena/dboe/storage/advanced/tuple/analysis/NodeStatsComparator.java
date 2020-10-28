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

import java.util.Comparator;

import org.apache.jena.ext.com.google.common.collect.ComparisonChain;

/**
 * Compare NodeStats instances by selectivty w.r.t. componentWeights
 *
 * Uses a very simple heuristic
 * <ol>
 * <li>Number of matched components</li>
 * <li>Component weight</li>
 * <li>Depth of the index node; e.g. prefer ([SP] ->[]) over ([S] -> ([P] -> [])</li>
 * <li>On tie: node id for determinism</li>
 * </ol>
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 */
public class NodeStatsComparator<D, C>
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
        int[] matchesA = a.getMatchedConstraintIdxSet().stream().mapToInt(x -> x).toArray();
        int[] matchesB = b.getMatchedConstraintIdxSet().stream().mapToInt(x -> x).toArray();

        int result = ComparisonChain.start()
            .compare(matchesB.length, matchesA.length) // greater length =>  more covers => better
            .compare(matchesB, matchesA, (x, y) -> compareIndirect(x, y, componentWeights)) // higher weight => better
            .compare(a.getAccessor().depth(), b.getAccessor().depth()) // lower depth => better
            .compare(a.getAccessor().id(), b.getAccessor().id()) // deterministic tie breaker
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