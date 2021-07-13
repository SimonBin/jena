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

package org.apache.jena.dboe.storage.advanced.tuple.api;

/**
 *
 * @author Claus Stadler 11/09/2020
 */
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class TupleAccessorQuadAnyToNull
    extends TupleAccessorQuad
{
    public static final TupleAccessorQuadAnyToNull INSTANCE = new TupleAccessorQuadAnyToNull();

    @Override
    public Node get(Quad quad, int idx) {
        return getComponent(quad, idx);
    }


    /**
     * Surely there is some common util method somewhere?
     *
     *
     * @return
     */
    public static Node getComponent(Quad quad, int idx) {
        switch(idx) {
        case 0: return anyToNull(quad.getSubject());
        case 1: return anyToNull(quad.getPredicate());
        case 2: return anyToNull(quad.getObject());
        case 3: return anyToNull(quad.getGraph());
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a quad");
        }
    }

    public static Node anyToNull(Node n) {
        return Node.ANY.equals(n) ? null : n;
    }


}
