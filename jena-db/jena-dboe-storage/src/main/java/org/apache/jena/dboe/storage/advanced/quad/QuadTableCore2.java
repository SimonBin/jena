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

import org.apache.jena.dboe.storage.advanced.tuple.api.TupleTableCore2;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Forwarding QuadTableCore that additionally tracks quads in a secondary QuadTableCore
 * Calling .find() with Node.ANY in all places yields a stream from the secondary table instead
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class QuadTableCore2
    extends TupleTableCore2<Quad, Node, QuadTableCore>
    implements QuadTableCore
{
    public QuadTableCore2(QuadTableCore primary, QuadTableCore secondary) {
        super(primary, secondary);
    }

    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        boolean matchesAny = Node.ANY.matches(g) && Node.ANY.matches(s) && Node.ANY.matches(p) && Node.ANY.matches(o);
        Stream<Quad> result = matchesAny
                ? secondary.find(g, s, p, o)
                : primary.find(g, s, p, o);
        return result;
    }

    @Override
    public Stream<Node> listGraphNodes() {
        return primary.listGraphNodes();
    }
}
