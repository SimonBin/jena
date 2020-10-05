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

package org.apache.jena.dboe.storage.advanced.triple;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.TupleTableFromStorageNodeWithCodecBase;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodecBased;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Adaption of a tuple table to the domain of quads
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <V>
 */
public class TripleTableFromStorageNodeWithCodec<D2, C2, V>
    extends TupleTableFromStorageNodeWithCodecBase<Triple, Node, D2, C2, V>
    implements TripleTableCore, TupleCodecBased<Triple, Node, D2, C2>
{
    public TripleTableFromStorageNodeWithCodec(
            TupleCodec<Triple, Node, D2, C2> tupleCodec,
            StorageNodeMutable<D2, C2, V> rootStorageNode,
            V store) {
        super(tupleCodec, rootStorageNode, store);
    }

    public static <V, D2, C2> TripleTableFromStorageNodeWithCodec<D2, C2, V> create(
            TupleCodec<Triple, Node, D2, C2> tupleCodec,
            StorageNodeMutable<D2, C2, V> rootStorageNode) {
        V store = rootStorageNode.newStore();
        return new TripleTableFromStorageNodeWithCodec<D2, C2, V>(tupleCodec, rootStorageNode, store);
    }

    @Override
    public TupleCodec<Triple, Node, D2, C2> getTupleCodec() {
        return tupleCodec;
    }

    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
//        return newFinder().eq(0, s).eq(1, p).eq(2, o).stream();
        return newFinder()
                .eq(0, TupleTableCore.anyToNull(s))
                .eq(1, TupleTableCore.anyToNull(p))
                .eq(2, TupleTableCore.anyToNull(o))
                .stream();
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
