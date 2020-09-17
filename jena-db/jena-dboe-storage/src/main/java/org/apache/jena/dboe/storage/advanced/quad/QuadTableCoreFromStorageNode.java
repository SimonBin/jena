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
package org.apache.jena.dboe.storage.advanced.quad;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.TupleTableFromStorageNodeBase;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Adaption of a tuple table to the domain of quads
 *
 * @author raven
 *
 * @param <V>
 */
public class QuadTableCoreFromStorageNode<V>
    extends TupleTableFromStorageNodeBase<Quad, Node, V>
    implements QuadTableCore
{
    public QuadTableCoreFromStorageNode(
            StorageNodeMutable<Quad, Node, V> rootStorageNode,
            V store) {
        super(rootStorageNode, store);
    }

    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        return newFinder()
                .eq(0, TupleTableCore.anyToNull(s))
                .eq(1, TupleTableCore.anyToNull(p))
                .eq(2, TupleTableCore.anyToNull(o))
                .eq(3, TupleTableCore.anyToNull(g))
                .stream();
    }

    @Override
    public Stream<Node> listGraphNodes() {
        return newFinder().projectOnly(3).distinct().stream();
    }


    public static <V> QuadTableCoreFromStorageNode<V> create(StorageNodeMutable<Quad, Node, V> rootStorageNode) {
        V store = rootStorageNode.newStore();
        return new QuadTableCoreFromStorageNode<V>(rootStorageNode, store);
    }

    // TODO We need to be wary of nulls / any!!!

//    @Override
//    public ResultStreamer<Quad, Node, Tuple<Node>> find(TupleQuery<Node> tupleQuery) {
//        NodeStats<Quad, Node> bestMatch = TupleQueryAnalyzer.analyze(tupleQuery, storeAccessor);
//        ResultStreamerBinder<Quad, Node, Tuple<Node>> binder = TupleQueryAnalyzer.createResultStreamer(
//                bestMatch,
//                tupleQuery,
//                TupleAccessorQuadAnyToNull.INSTANCE);
//
//        return binder.bind(store);
//    }

}
