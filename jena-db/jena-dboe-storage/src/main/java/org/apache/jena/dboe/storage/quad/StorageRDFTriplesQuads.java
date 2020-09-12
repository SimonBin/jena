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

import java.util.Iterator;

import org.apache.jena.dboe.storage.StorageRDF;
import org.apache.jena.dboe.storage.triple.TripleTableCore;
import org.apache.jena.dboe.storage.triple.TripleTableCoreFromQuadTable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

/**
 * An basic implementation of StorageRDF that forwards all calls as triples and quads
 * to TripleTableCore and QuadTableCore instances
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class StorageRDFTriplesQuads
    implements StorageRDF
{
    protected TripleTableCore tripleTable;
    protected QuadTableCore quadTable;

    public StorageRDFTriplesQuads(TripleTableCore tripleTable, QuadTableCore quadTable) {
        super();
        this.tripleTable = tripleTable;
        this.quadTable = quadTable;
    }

    /**
     * Create an instance of this class using only a single quadTable that will
     * hold triples and quads.
     *
     * @param quadTable
     * @return
     */
    public static StorageRDF createWithQuadsOnly(QuadTableCore quadTable) {
        return new StorageRDFTriplesQuads(
                new TripleTableCoreFromQuadTable(quadTable, Quad.defaultGraphIRI),
                new QuadTableWithHiddenGraphs(quadTable, Quad::isDefaultGraph));
    }


    public TripleTableCore getTripleTable() {
        return tripleTable;
    }

    public QuadTableCore getQuadTable() {
        return quadTable;
    }

    /*
     * Triples
     */

    @Override
    public void add(Triple triple) {
        tripleTable.add(triple);
    }

    @Override
    public void delete(Triple triple) {
        tripleTable.delete(triple);
    }

    @Override
    public boolean contains(Triple triple) {
        return tripleTable.contains(triple);
    }

    @Override
    public Iterator<Triple> find(Node s, Node p, Node o) {
        return tripleTable.find(s, p, o).iterator();
    }

    /*
     * Quads
     */

    @Override
    public void add(Quad quad) {
        quadTable.add(quad);
    }

    @Override
    public void delete(Quad quad) {
        quadTable.delete(quad);
    }

    @Override
    public boolean contains(Quad quad) {
        return quadTable.contains(quad);
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        return quadTable.find(g, s, p, o).iterator();
    }


    /*
     * To Triples
     */

    @Override
    public void add(Node s, Node p, Node o) {
        add(Triple.create(s, p, o));
    }

    @Override
    public void delete(Node s, Node p, Node o) {
        delete(Triple.create(s, p, o));
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return contains(Triple.create(s, p, o));
    }


    /*
     * To Quads
     */

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        add(Quad.create(g, s, p, o));
    }


    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        delete(Quad.create(g, s, p, o));
    }

    @Override
    public boolean contains(Node g, Node s, Node p, Node o) {
        return contains(Quad.create(g, s, p, o));
    }

}
