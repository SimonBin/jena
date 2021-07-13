package org.apache.jena.dboe.storage.advanced.hypertrie;

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.core.TripleStorages;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessors;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
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

        Object subStore = accessor.getStoreForSliceByComponentByValue(store, 0, NodeFactory.createURI("http://www.example.org/e4"));
        HyperTrieAccessor<Node> sp = accessor.getAccessorForComponent(0);
        Set<Node> e4Preds = sp.getValuesForComponent(subStore, 1);
        System.out.println(e4Preds);

        Object poSlice = sp.getStoreForSliceByComponentByValue(subStore, 1, RDF.Nodes.type);
        HyperTrieAccessor<Node> spo = sp.getAccessorForComponent(1);
        Set<Node> os = spo.getValuesForComponent(poSlice, 2);
        System.out.println(os);

        // Should the API allow slicing a leaf set???
        // So given a leaf set of {1, 2, 3} we could slice on its indexed component;
        // if sliced by e.g. 2 it would given the singular set {2}
        // if sliced by e.g. 4 it would give an empty set {}
        // Then again, this would cause every value to get wrapped in an extra object which we want
        // to avoid anyway - so this feature has no priority
        if (false) {
            Object posSlice = spo.getStoreForSliceByComponentByValue(poSlice, 2, NodeFactory.createURI("http://dbpedia.org/resource/Unicorn"));
            HyperTrieAccessor<Node> posSliceAccess = sp.getAccessorForComponent(2);
            Set<Node> leafSlice = posSliceAccess.getValuesForComponent(posSlice, 0);
            System.out.println(leafSlice);
        }

    }
}
