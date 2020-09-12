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

package org.apache.jena.dboe.storage.quad;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.tuple.TupleTableCore;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

/**
 * Core interface for a collection of {@link Quad} instances.
 *
 * Can serve on multiple API contracts by either allowing/exposing
 * or disallowing/hiding the default graph
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public interface QuadTableCore
    extends TupleTableCore<Quad, Node>
{
    /**
     * Find quads matching the given nodes
     *
     * @return The stream of matching {@link Quad}s
     */
    Stream<Quad> find(Node g, Node s, Node p, Node o);

    @Override
    default Stream<Quad> findTuples() {
        return find(null, null, null, null);
    }

    @Override
    default Stream<Quad> findTuples(Node... pattern) {
        return find(
                TupleTableCore.nullToAny(pattern[0]),
                TupleTableCore.nullToAny(pattern[1]),
                TupleTableCore.nullToAny(pattern[2]),
                TupleTableCore.nullToAny(pattern[3]));
    }

//    default <T> Stream<Quad> find(T obj, TupleAccessor<? super T, ? extends Node> accessor) {
//        return find(
//            accessor.get(obj, 3),
//            accessor.get(obj, 0),
//            accessor.get(obj, 1),
//            accessor.get(obj, 2));
//    }
    default Stream<Quad> find() {
        return find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
    }

    @Override
    default TupleAccessor<Quad, Node> getTupleAccessor() {
        return TupleAccessorQuad.INSTANCE;
    }

    /**
     * Yield a stream of distinct graph nodes
     *
     * Table implementations may offer more efficient access to graph nodes than
     * what is possible via the find method.
     *
     * DevNote: Should be based on a general to be introduced TupleFinder interface that allows
     * specifying constraints (equal / range), the component(s) to project and whether to apply distinct
     *
     * Be aware that items equal to Quad.isDefaultGraph
     * may be acceptable for intermediate processing but they must not
     * not end up in the rest of the Jena machinery.
     *
     * @return The stream of graphNodes contained in this instance
     */
    default Stream<Node> listGraphNodes() {
        return newFinder().project(3).distinct(true).plain().stream();
    }

    default int getRank() {
        return 4;
    }

    default boolean isEmpty() {
        return !find()
                .findAny().isPresent();
    }

    default boolean contains(Quad quad) {
        boolean result = find(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject())
            .findAny().isPresent();
        return result;
    }

    default void deleteGraph(Node g) {
        List<Quad> list = find(g, Node.ANY, Node.ANY, Node.ANY).collect(Collectors.toList());
        for (Quad quad : list) {
            delete(quad);
        }
    }
}
