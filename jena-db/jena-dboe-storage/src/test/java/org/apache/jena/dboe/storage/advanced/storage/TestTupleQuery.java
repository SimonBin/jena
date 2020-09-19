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
package org.apache.jena.dboe.storage.advanced.storage;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuadAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQueryImpl;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.NodeStats;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessorImpl;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.TupleQueryAnalyzer;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Alt2;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerBinder;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Test;

public class TestTupleQuery {

    @Test
    public void testTupleQueryMachinery() {

        /*
         * The following creates a nested storage layout:
         * The numbers correspond to the ids assigned to the nodes via depth first pre order traversal
         * The nodes labeled g s p o (i.e. 1 3 4 5) are the index nodes that correspond to these components.
         * x and y fork the tree into alternatives
         * The nodes 6 and 7 correspond to the LinkedHashSets
         *
         *
         * x   g   y   s   p   o
         * 0 - 1 - 2 - 3 - 4 - 5
         * \      \
         *  7      6
         */

        StorageNodeMutable<Quad, Node, Alt2<Map<Node, Alt2<Map<Node, Map<Node, Map<Node, Quad>>>, Set<Quad>>>, Set<Quad>>>
        storage =
            alt2(
                innerMap(3, LinkedHashMap::new,
                    alt2(
                        innerMap(0, LinkedHashMap::new,
                            innerMap(1, LinkedHashMap::new,
                                leafMap(2, LinkedHashMap::new, TupleAccessorQuad.INSTANCE))),
                        leafSet(LinkedHashSet::new, TupleAccessorQuad.INSTANCE))),
                leafSet(LinkedHashSet::new, TupleAccessorQuad.INSTANCE));


        System.out.println("Storage structure: " + storage);
        Alt2<Map<Node, Alt2<Map<Node, Map<Node, Map<Node, Quad>>>, Set<Quad>>>, Set<Quad>>
        store = storage.newStore();

        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        System.out.println("Performing inserts");
        storage.add(store, q1);
        storage.add(store, q2);
        storage.add(store, q3);
        storage.add(store, q4);

        /*
         * StorageNode objects only have a low level API. They have methods to get data in and out of a store object,
         * but they do not have methods for cross-StorgeNode operations.
         * While they have links to their children, they do not link to their parents.
         *
         * StoreAccessor is a 1:1 wrapping tree structure with a high level API.
         * Each StoreAcessor node supports computing a cartesian product under constraints starting in the data
         * of the root stores
         *
         */
        StoreAccessor<Quad, Node> storeAccessor = StoreAccessorImpl.createForStorage(storage);


        TupleQuery<Node> tupleQuery = new TupleQueryImpl<>(4);
        tupleQuery
            .setDistinct(true)
            .setConstraint(0, q1.getSubject())
//            .setConstraint(2, q1.getObject())
            .setProject(0, 1, 2, 3);
        // tupleQuery.setConstraint(3, RDF.Nodes.type);


        NodeStats<Quad, Node> bestMatch = TupleQueryAnalyzer.analyze(
                tupleQuery,
                storeAccessor,
                new int[] {10, 10, 1, 100});
        System.out.println("Best match: " + bestMatch);

        ResultStreamerBinder<Quad, Node, Tuple<Node>> rs = TupleQueryAnalyzer.createResultStreamer(
                bestMatch,
                tupleQuery,
                TupleAccessorQuadAnyToNull.INSTANCE);

        /*
         * All operations so far considered the storage structure (schema) but were data-blind
         * The result streamer encapsulates the plan for executing a tuple query
         * Here the plan is bound to concrete data
         */
        rs.bind(store).streamAsTuple()
            .forEach(tuple -> System.out.println("GOT TUPLE: " + tuple));

        rs.bind(store).streamAsDomainObject()
            .forEach(quad -> System.out.println("GOT QUAD: " + quad));
    }
}
