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

import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleTableCore;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Core interface for a collection of {@link Triple} instances
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public interface TripleTableCore
    extends TupleTableCore<Triple, Node>
{
    Stream<Triple> find(Node s, Node p, Node o);

    @Override
    default Stream<Triple> findTuples() {
        return find(null, null, null);
    }

    @Override
    default Stream<Triple> findTuples(List<Node> pattern) {
        return find(
                TupleTableCore.nullToAny(pattern.get(0)),
                TupleTableCore.nullToAny(pattern.get(1)),
                TupleTableCore.nullToAny(pattern.get(2)));
    }

//    public static Node anyToNull(Node n) {
//        return Node.ANY.equals(n) ? null : n;
//    }

    default int getRank() {
        return 3;
    }

    default boolean isEmpty() {
        return !find(Node.ANY, Node.ANY, Node.ANY)
                .findAny().isPresent();
    }

    default boolean contains(Triple triple) {
        return find(triple.getSubject(), triple.getPredicate(), triple.getObject())
                .findAny().isPresent();
    }

    @Override
    default TupleAccessor<Triple, Node> getTupleAccessor() {
        return TupleAccessorTriple.INSTANCE;
    }

//  Note: We may want to add try-with-resources in order to ensure closing underlying resources
//        boolean result;
//        try (Stream<Triple> stream = find(triple.getSubject(), triple.getPredicate(), triple.getObject())) {
//            result = stream.findAny().isPresent();
//        }
//        return result;
}
