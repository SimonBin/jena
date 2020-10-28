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

package org.apache.jena.dboe.storage.advanced.core;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafSet;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.function.Supplier;

import org.apache.jena.dboe.storage.StorageRDF;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore2;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromSet;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromStorageNode;
import org.apache.jena.dboe.storage.advanced.quad.StorageRDFTriplesQuads;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore2;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromSet;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuadAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeMutable;
import org.apache.jena.dboe.storage.simple.StoragePrefixesMem;
import org.apache.jena.dboe.storage.system.DatasetGraphStorage;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.TransactionalLock;
import org.apache.jena.sparql.expr.NodeValue;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class DatasetGraphFactoryOrdered {

    public static DatasetGraph createOrderAwareDatasetGraph() {
        return createOrderAwareDatasetGraph(true, true);
    }

    public static DatasetGraph createOrderAwareDatasetGraph(
            boolean strictOrderOnTriples,
            boolean strictOrderOnQuads) {

        StorageRDF storage = createOrderAwareStorageRDF(strictOrderOnTriples, strictOrderOnQuads);
        DatasetGraph result = new DatasetGraphStorage(
                storage,
                new StoragePrefixesMem(),
                TransactionalLock.createMRSW());

        return result;
    }


    /** Can be used in specification of a storage layout
     *  in conjunction with TreeSets */
    public static class NodeComparatorViaNodeValue implements Comparator<Node> {
        @Override
        public int compare(Node o1, Node o2)
        {
            return NodeValue.compareAlways(NodeValue.makeNode(o1), NodeValue.makeNode(o2));
        }
    }


    /** Kept as a temporary reference to show the difference between the approach on the
    /* domain tables vs the tuple storage */
    public static DatasetGraph createOrderPreservingOld(boolean strictOrderOnQuads, boolean strictOrderOnTriples) {
        Supplier<TripleTableCore> tripleTableSupplier = strictOrderOnTriples
                ? () -> new TripleTableCore2(new TripleTableCoreFromNestedMapsImpl(), new TripleTableCoreFromSet())
                : () -> new TripleTableCoreFromNestedMapsImpl();

        QuadTableCore quadTable = new QuadTableCoreFromMapOfTripleTableCore(tripleTableSupplier);

        if (strictOrderOnQuads) {
            quadTable = new QuadTableCore2(quadTable, new QuadTableCoreFromSet());
        }

        StorageRDF storage = StorageRDFTriplesQuads.createWithQuadsOnly(quadTable);
        DatasetGraph result = new DatasetGraphStorage(storage, new StoragePrefixesMem(), TransactionalLock.createMRSW());
        return result;
    }



    public static <T> StorageNodeMutable<T, Node, ?> createTripleStorage(
            boolean strictOrder,
            TupleAccessor<T, Node> tupleAccessors) {

        // Preliminary result is nested linked hash maps - S -> P -> O
        StorageNodeMutable<T, Node, ?> result =
            innerMap(0, LinkedHashMap::new,
                    innerMap(1, LinkedHashMap::new,
                        leafMap(2, LinkedHashMap::new, tupleAccessors)));

        // Note that predicates could be stored sorted by name like this:
        // innerMap(1, MapSupplier.forTreeMap(new NodeComparatorViaNodeValue()), ...

        if (strictOrder) {
            // Modify the result to keep all triples in an additional linked hash set
            // Requests without constraints
            // (i.e. ?s ?p ?o) will be served from this set
            result = alt2(result, leafSet(LinkedHashSet::new, tupleAccessors));
        }

        return result;
    }




    public static StorageRDF createOrderAwareStorageRDF(
            boolean strictOrderOnTriples,
            boolean strictOrderOnQuads) {

        StorageNodeMutable<Triple, Node, ?> tripleStorage = createTripleStorage(
                strictOrderOnTriples, TupleAccessorTripleAnyToNull.INSTANCE);

        StorageNodeMutable<Quad, Node, ?> quadStorage = QuadStorages.createQuadStorage(
                strictOrderOnQuads, TupleAccessorQuadAnyToNull.INSTANCE);

        TripleTableCore tripleTable = TripleTableFromStorageNode.create(tripleStorage);
        QuadTableCore quadTable = QuadTableCoreFromStorageNode.create(quadStorage);

        return new StorageRDFTriplesQuads(tripleTable, quadTable);
    }

    /*
     * Testing
     */

    public static StorageRDF createTestStorageRDF() {
        return createOrderAwareStorageRDF(true, true);
    }

    public static DatasetGraph createTestDatasetGraph() {
        return new DatasetGraphStorage(createTestStorageRDF(),
                new StoragePrefixesMem(), TransactionalLock.createMRSW());
    }
}
