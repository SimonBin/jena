package org.apache.jena.dboe.storage.advanced.tuple.engine;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.altN;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TripleStorages;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;

public class MainEngineTest {

    public static void main(String[] args) throws IOException {


        for (int i = 0; i < 100; ++i) {
            doWork();
        }


//        RDFDataMgr.write(System.out, model, RDFFormat.TRIG_PRETTY);

    }


    public static void doWork() {
        Stopwatch loadingTimeSw = Stopwatch.createStarted();


        Model model;
        StorageNodeMutable<Triple, Node, ?> storage = null;
        Object store = null;
        if (true) {

            QC.setFactory(ARQ.getContext(), execCxt -> {
                return new OpExecutorTupleEngine(execCxt);
            });

            storage = TripleStorages.createHyperTrieStorage();

//            StorageNodeMutable<Triple, Node, ?> storage = createConventionalStorage();
            TripleTableFromStorageNode<?> tripleTableCore = TripleTableFromStorageNode.create(storage);
            store = tripleTableCore.getStore();

            model = ModelFactory.createModelForGraph(
                    GraphWithAdvancedFind.create(tripleTableCore));
        } else {
            model = ModelFactory.createDefaultModel();
        }


        RDFDataMgr.read(model, "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt");

        System.out.println("Loading time: " + loadingTimeSw);


        Query query = QueryFactory.create("SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }", Syntax.syntaxSPARQL_10);
//        Query query = QueryFactory.create("SELECT DISTINCT ?a ?b WHERE { ?a a ?b }", Syntax.syntaxSPARQL_10);

        BasicPattern bgp = ((ElementTriplesBlock)((ElementGroup)query.getQueryPattern()).get(0)).getPattern();
        System.out.println(bgp);


        Stopwatch executionTimeSw = Stopwatch.createStarted();

        EinsteinSummation.<Triple, Node, Triple>einsum(
                storage, store, bgp.getList(), TupleAccessorTripleAnyToNull.INSTANCE, Node::isVariable);

        System.out.println("Execution time: " + executionTimeSw);

    }



    public static void main2(String[] args) throws IOException {


        StorageNodeMutable<Triple, Node, ?> storage =
            altN(Arrays.asList(
            // spo
            innerMap(0, HashMap::new,
                innerMap(1, HashMap::new,
                    leafMap(2, HashMap::new, TupleAccessorTriple.INSTANCE)))
            ,
            // ops
            innerMap(2, HashMap::new,
                innerMap(1, HashMap::new,
                    leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
            ,
//            // osp (using a somewhat odd index for testing)
//            innerMap(2, HashMap::new,
//                    innerMap(0, HashMap::new,
//                        leafMap(1, TupleAccessorTriple.INSTANCE, HashMap::new)))

            // p
            innerMap(1, HashMap::new,
                leafSet(HashSet::new, TupleAccessorTriple.INSTANCE))
            ,
            // pos
            innerMap(1, HashMap::new,
                    innerMap(2, HashMap::new,
                        leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
            //,
            //leafSet(TupleAccessorTriple.INSTANCE, HashSet::new)

            ));

/*
        storage =
                altN(Arrays.asList(
                    // ps
                    innerMap(1, HashMap::new,
                        innerMap(0, HashMap::new,
                            leafMap(2, TupleAccessorTriple.INSTANCE, HashMap::new)))
                    ,
                    // os
                    innerMap(2, HashMap::new,
                            innerMap(1, HashMap::new,
                                leafMap(0, TupleAccessorTriple.INSTANCE, HashMap::new)))

                    ));
*/
        Model model;
        if (true) {

            QC.setFactory(ARQ.getContext(), execCxt -> {
                return new OpExecutorTupleEngine(execCxt);
            });

            model = ModelFactory.createModelForGraph(GraphWithAdvancedFind.create(TripleTableFromStorageNode.create(storage)));
        } else {
            model = ModelFactory.createDefaultModel();
        }




        RDFDataMgr.read(model, "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt");

        Iterator<String> it = Files.readAllLines(Paths.get("/home/raven/research/jena-vs-tentris/data/swdf/SWDF-Queries.txt")).iterator();

        Stopwatch swTotal = Stopwatch.createStarted();
        while (it.hasNext()) {
            String queryStr = it.next();

//            queryStr = "SELECT DISTINCT  ?d ?e\n" +
//                    "WHERE\n" +
//                    "  { ?a  a  <http://data.semanticweb.org/ns/swc/ontology#IW3C2Liaison> .\n" +
//                    "    ?c  a  ?d .\n" +
//                    "    ?a  ?e        ?c\n" +
//                    "  }";

//            queryStr = "SELECT DISTINCT  *\n" +
//                    "WHERE\n" +
//                    "  { ?a  a  <http://data.semanticweb.org/ns/swc/ontology#IW3C2Liaison> .\n" +
//                    "  }";

            // ?a ?b ?c ?d ?e

//            queryStr = "SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }";
//            queryStr = "SELECT ?b ?d ?e WHERE { ?a a ?b . ?a ?e ?c . ?c a ?d . }";
//            queryStr = "SELECT DISTINCT ?p WHERE { _:s ?p _:o }";
            Query query = QueryFactory.create(queryStr);


            System.out.println("Executing " + query);
            Stopwatch sw = Stopwatch.createStarted();
            try (QueryExecution qe = QueryExecutionFactory.create(query, model)) {
                qe.setTimeout(30000);
                ResultSet rs = qe.execSelect();
                System.out.println("Result set size: " + ResultSetFormatter.consume(rs));
//                ResultSetFormatter.outputAsTSV(System.out, rs);
                System.out.println();

//                System.out.println(ResultSetFormatter.asText(rs));
            }
            System.out.println(sw + " - " + swTotal);
        }


    }

}
