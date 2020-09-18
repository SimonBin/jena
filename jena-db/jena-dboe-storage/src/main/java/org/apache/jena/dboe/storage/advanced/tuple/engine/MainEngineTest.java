package org.apache.jena.dboe.storage.advanced.tuple.engine;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;

import java.util.HashMap;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.main.QC;

public class MainEngineTest {

    public static void main(String[] args) {
        QC.setFactory(ARQ.getContext(), execCxt -> {
            return new OpExecutorTupleEngine(execCxt);
        });

        StorageNodeMutable<Triple, Node, ?> storage = innerMap(0, HashMap::new,
            innerMap(1, HashMap::new,
                leafMap(2, TupleAccessorTriple.INSTANCE, HashMap::new)));

        Model model = ModelFactory.createModelForGraph(GraphWithAdvancedFind.create(TripleTableFromStorageNode.create(storage)));
        RDFDataMgr.read(model, "/home/raven/Projects/limbo/git/metadata-catalog/catalog.all.ttl");

        try (QueryExecution qe = QueryExecutionFactory.create("SELECT DISTINCT ?t { ?s a ?t }", model)) {
            ResultSet rs = qe.execSelect();
            System.out.println(ResultSetFormatter.asText(rs));
        }

    }

}
