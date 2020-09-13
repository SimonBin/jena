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

import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.QuadTable;

/**
 * A triple table view on a the content of a specific graph in a {@link QuadTable}
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class TripleTableCoreFromQuadTable
    implements TripleTableCore
{
    protected QuadTableCore quadTable;
    protected Node graphNode;

    public TripleTableCoreFromQuadTable(QuadTableCore quadTable, Node graphNode) {
        super();
        this.quadTable = quadTable;
        this.graphNode = graphNode;
    }

    @Override
    public void clear() {
        quadTable.deleteGraph(graphNode);
    }

    @Override
    public void add(Triple triple) {
        quadTable.add(Quad.create(graphNode, triple));
    }

    @Override
    public void delete(Triple triple) {
        quadTable.delete(Quad.create(graphNode, triple));
    }

    @Override
    public Stream<Triple> find(Node s, Node p, Node o) {
        return quadTable.find(graphNode, s, p, o)
                .map(Quad::asTriple);
    }
}