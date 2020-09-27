package org.apache.jena.dboe.storage.advanced.storage;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessors;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TripleStorages;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

public class TestHyperTrieAccessor {
    @Test
    public void testHyperTrieAccessor() {
        Model m = RDFDataMgr.loadModel("tentris-example.ttl");


        StorageNodeMutable<Triple, Node, ?> storageNode = TripleStorages.createHyperTrieStorage(TupleAccessorTriple.INSTANCE);
        Object store = storageNode.newStore();

        m.getGraph().find().forEachRemaining(t -> storageNode.addRaw(store, t));

        HyperTrieAccessor<Node> accessor = HyperTrieAccessors.index(storageNode);


        Set<Node> subjects = accessor.getValuesForComponent(store, 0);
        Set<Node> predicates = accessor.getValuesForComponent(store, 1);
        Set<Node> objects = accessor.getValuesForComponent(store, 2);

        System.out.println(subjects);
        System.out.println(predicates);
        System.out.println(objects);
    }
}
