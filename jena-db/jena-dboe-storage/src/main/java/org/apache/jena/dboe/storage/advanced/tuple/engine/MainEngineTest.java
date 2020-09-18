package org.apache.jena.dboe.storage.advanced.tuple.engine;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.altN;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.main.QC;

public class MainEngineTest {

    public static void main(String[] args) throws IOException {
        QC.setFactory(ARQ.getContext(), execCxt -> {
            return new OpExecutorTupleEngine(execCxt);
        });


        StorageNodeMutable<Triple, Node, ?> storage =
            altN(Arrays.asList(
            // spo
            innerMap(0, HashMap::new,
                innerMap(1, HashMap::new,
                    leafMap(2, TupleAccessorTriple.INSTANCE, HashMap::new)))
            ,
          // ops (this one seems to fail)
          innerMap(2, HashMap::new,
                  innerMap(1, HashMap::new,
                      leafMap(0, TupleAccessorTriple.INSTANCE, HashMap::new)))
              ,
//            // osp (using a somewhat odd index for testing
//            innerMap(2, HashMap::new,
//                    innerMap(0, HashMap::new,
//                        leafMap(1, TupleAccessorTriple.INSTANCE, HashMap::new)))
            // pos
            innerMap(1, HashMap::new,
                    innerMap(2, HashMap::new,
                        leafMap(0, TupleAccessorTriple.INSTANCE, HashMap::new)))

            ));
        Model model = ModelFactory.createModelForGraph(GraphWithAdvancedFind.create(TripleTableFromStorageNode.create(storage)));
        RDFDataMgr.read(model, "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt");

        Iterator<String> it = Files.readAllLines(Paths.get("/home/raven/research/jena-vs-tentris/data/swdf/SWDF-Queries.txt")).iterator();

        while (it.hasNext()) {
            String queryStr = it.next();

            queryStr = "SELECT DISTINCT  ?d ?e\n" +
                    "WHERE\n" +
                    "  { ?a  a  <http://data.semanticweb.org/ns/swc/ontology#IW3C2Liaison> .\n" +
                    "    ?c  a  ?d .\n" +
                    "    ?a  ?e        ?c\n" +
                    "  }";

//            queryStr = "SELECT DISTINCT  *\n" +
//                    "WHERE\n" +
//                    "  { ?a  a  <http://data.semanticweb.org/ns/swc/ontology#IW3C2Liaison> .\n" +
//                    "  }";

            Query query = QueryFactory.create(queryStr);


            System.out.println("Executing " + query);
            try (QueryExecution qe = QueryExecutionFactory.create(query, model)) {
                ResultSet rs = qe.execSelect();
                System.out.println("Result set size: " + ResultSetFormatter.consume(rs));
                System.out.println();

                //System.out.println(ResultSetFormatter.asText(rs));
            }
        }

    }

}
