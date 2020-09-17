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

package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TupleAccessorTripleAnyToNull
    extends TupleAccessorTriple
{
    public static final TupleAccessorTripleAnyToNull INSTANCE = new TupleAccessorTripleAnyToNull();

    @Override
    public Node get(Triple quad, int idx) {
        return getComponent(quad, idx);
    }

    /**
     * Surely there is some common util method somewhere?
     *
     *
     * @return
     */
    public static Node getComponent(Triple quad, int idx) {
        switch(idx) {
        case 0: return TupleAccessorQuadAnyToNull.anyToNull(quad.getSubject());
        case 1: return TupleAccessorQuadAnyToNull.anyToNull(quad.getPredicate());
        case 2: return TupleAccessorQuadAnyToNull.anyToNull(quad.getObject());
        default: throw new IndexOutOfBoundsException("Cannot access index " + idx + " of a triple");
        }
    }



}
