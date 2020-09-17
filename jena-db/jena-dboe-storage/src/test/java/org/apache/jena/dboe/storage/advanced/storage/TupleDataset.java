package org.apache.jena.dboe.storage.advanced.storage;

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
import org.apache.jena.dboe.storage.advanced.quad.QuadTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.quad.StorageRDFTriplesQuads;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore2;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromSet;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.MapSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.simple.StoragePrefixesMem;
import org.apache.jena.dboe.storage.system.DatasetGraphStorage;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.TransactionalLock;
import org.apache.jena.sparql.expr.NodeValue;

public class TupleDataset {
    public static DatasetGraph createOrderPreserving() {
        return createOrderPreserving(false, false);
    }

    public static DatasetGraph createOrderPreserving(boolean strictOrderOnQuads, boolean strictOrderOnTriples) {
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


    public static class NodeComparatorViaNodeValue implements Comparator<Node> {
        @Override
        public int compare(Node o1, Node o2)
        {
            return NodeValue.compareAlways(NodeValue.makeNode(o1), NodeValue.makeNode(o2));
        }
    }


    public static <T> StorageNodeMutable<T, Node, ?> createTripleStorage(TupleAccessor<T, Node> tupleAccessors) {
        StorageNodeMutable<T, Node, ?> result =
            alt2(
                innerMap(0, LinkedHashMap::new,
                    innerMap(1, MapSupplier.forTreeMap(new NodeComparatorViaNodeValue()),
                        leafMap(2, tupleAccessors, LinkedHashMap::new))),
                leafSet(tupleAccessors, LinkedHashSet::new));

        return result;
    }

    public static StorageRDF createTestStorage() {

        StorageNodeMutable<Triple, Node, ?> tripleStorage = createTripleStorage(TupleAccessorTriple.INSTANCE);

        StorageNodeMutable<Quad, Node, ?> quadStorage = alt2(
            innerMap(3, LinkedHashMap::new,
                    createTripleStorage(TupleAccessorQuad.INSTANCE)),
            leafSet(TupleAccessorQuad.INSTANCE, LinkedHashSet::new));


        TripleTableCore tripleTable = TripleTableFromStorageNode.create(tripleStorage);
        QuadTableCore quadTable = QuadTableFromStorageNode.create(quadStorage);
        return new StorageRDFTriplesQuads(tripleTable, quadTable);
    }

    public static DatasetGraph createTestDatasetGraph() {
        StorageRDF storage = createTestStorage();
        DatasetGraph result = new DatasetGraphStorage(storage, new StoragePrefixesMem(), TransactionalLock.createMRSW());
        return result;
    }


}
