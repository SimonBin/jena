package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNode;
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
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;

public class MainEngineTest {

    public static void main(String[] args) throws IOException {

        Model[] model = {null};
        Function<Query, QueryExecution> queryExecutor = null;

        // 0 = hyper trie, 1 = default model, 2 = tentris
        int mode = 2;

        switch (mode) {
        case 0:
            // swdf      Time until completion of a full run: 4.307 s - num queries: 203 num bindings: 1108714  avg qps: 47.12855026185931
            // watdiv-15 Time until completion of a full run: 650.0 ms - num queries: 45 num bindings: 79962  avg qps: 69.23268921300884

            model[0] = createHyperTrieBackedModel();
            queryExecutor = query -> {
                QueryExecution qe = QueryExecutionFactory.create(query, model[0]);
                StageBuilder.setGenerator(qe.getContext(), StageGeneratorHyperTrie
                         .create()
                         .parallel(true)
                         .bufferBindings(true)
                         .bufferStatsCallback(System.err::println)
                );
                return qe;
            };
            break;
        case 1:
            // swdf      Time until completion of a full run: 3.306 s - num queries: 203 num bindings: 1108714  avg qps: 61.39700720772283
            // watdiv-15 Time until completion of a full run: 2.308 s - num queries: 45 num bindings: 79962  avg qps: 19.500878896161467
            model[0] = ModelFactory.createDefaultModel();
            queryExecutor = query -> QueryExecutionFactory.create(query, model[0]);
            break;
        case 2:
            // watdiv-15 Time until completion of a full run: 108.1 ms - num queries: 45 num bindings: 0  avg qps: 416.1240178294161
            queryExecutor = query -> QueryExecutionFactory.createServiceRequest("http://localhost:9080/sparql", query);
            break;
        default:
            throw new RuntimeException("no mode with this id");
        }


        if (model[0] != null) {
//            String filename = "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt";
            String filename = "/home/raven/research/jena-vs-tentris/data/watdiv/watdiv-15.nt";

            Stopwatch loadingSw = Stopwatch.createStarted();
            System.out.println("Loading data " + filename);
            RDFDataMgr.read(model[0], filename);
            System.out.println("Loaded in " + loadingSw);
        }

//        Iterable<String> workloads = Arrays.asList(
//        		"SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }");

        Collection<String> workloads = Files.readAllLines(
//                Paths.get("/home/raven/research/jena-vs-tentris/data/swdf/SWDF-Queries.txt"));
                Paths.get("/home/raven/research/jena-vs-tentris/data/watdiv/WatDiv-Queries.txt"));


        for (int i = 0; i < 1; ++i) {
            doWork(queryExecutor, workloads);
        }
    }



    public static void doWork(Function<Query, QueryExecution> queryExecutor, Iterable<String> workloads) throws IOException {


        for (int j = 0; j < 10; ++j) {
            Stopwatch runTimeSw = Stopwatch.createStarted();

            int queryCounter = 0;
            long bindingCounter = 0;
            for(String queryStr : workloads) {
                ++queryCounter;

                Query query = QueryFactory.create(queryStr);

                Stopwatch executionTimeSw = Stopwatch.createStarted();
                try (QueryExecution qe = queryExecutor.apply(query)) {

                    ResultSet rs = qe.execSelect();
//                    long count = ResultSetFormatter.consume(rs);

                    // ResultSetFormatter.consume materializes all Model objects which
                    // may take significantly more time than just listing the bindings
                    long count = 0;
                    while (rs.hasNext()) {
                        rs.nextBinding();
                        ++count;
                    }

                    bindingCounter += count;
                    System.out.println("Execution time: " + executionTimeSw + " - result set size: " + count);
                }
            }

            double avgQueriesPerSecond = queryCounter / (double)runTimeSw.elapsed(TimeUnit.NANOSECONDS) * 1000000000.0;
            System.out.println("Time until completion of a full run: " + runTimeSw + " - num queries: " + queryCounter + " num bindings: " + bindingCounter + "  avg qps: " + avgQueriesPerSecond);
        }

    }



    public static Model createHyperTrieBackedModel() {
        StorageNodeMutable<Triple, Node, ?> storage = null;
        storage = TripleStorages.createHyperTrieStorage();
//      StorageNodeMutable<Triple, Node, ?> storage = createConventionalStorage();

        TripleTableFromStorageNode<?> tripleTableCore = TripleTableFromStorageNode.create(storage);

        Model result = ModelFactory.createModelForGraph(
              GraphFromTripleTableCore.create(tripleTableCore));

        return result;
    }



    public static void doWorkToBeDeleted() throws IOException {
        System.out.println("Using einsum approach");
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
                    GraphFromTripleTableCore.create(tripleTableCore));
        } else {
            model = ModelFactory.createDefaultModel();
        }


        RDFDataMgr.read(model, "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt");

        System.out.println("Loading time: " + loadingTimeSw);


        Collection<String> queryStrs = Files.readAllLines(Paths.get("/home/raven/research/jena-vs-tentris/data/swdf/SWDF-Queries.txt"))
                .stream().limit(50).collect(Collectors.toList());


//        Query query = QueryFactory.create("SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }", Syntax.syntaxSPARQL_10);
//        Query query = QueryFactory.create("SELECT DISTINCT ?a ?b WHERE { ?a a ?b }", Syntax.syntaxSPARQL_10);


        for (int j = 0; j < 1000; ++j) {
            Stopwatch runTimeSw = Stopwatch.createStarted();

            int queryCounter = 0;
            long bindingCounter = 0;
            for (String queryStr : queryStrs) {
                ++queryCounter;
                Query query = QueryFactory.create(queryStr, Syntax.syntaxSPARQL_10);


                BasicPattern bgp = ((ElementTriplesBlock)((ElementGroup)query.getQueryPattern()).get(0)).getPattern();
                Set<Var> projectVars = new HashSet<>(query.getProjectVars());
//                System.out.println(bgp);

                Stopwatch executionTimeSw = Stopwatch.createStarted();

                Stream<Binding> bindings = EinsteinSummation.einsum(storage, store, bgp, projectVars);

                if (query.isDistinct()) {
                    bindings = bindings.distinct();
                }

                long count = bindings.count();
                bindingCounter += count;
    //            Iterator<Binding> it = bindings.iterator();
    //            while (it.hasNext()) {
    //                Binding b = it.next();
    //                System.out.println(b);
    //            }

                System.out.println("Execution time: " + executionTimeSw + " - result set size: " + count);
            }
            double avgQueriesPerSecond = queryCounter / (double)runTimeSw.elapsed(TimeUnit.NANOSECONDS) * 1000000000.0;
            System.out.println("Time until completion of a full run: " + runTimeSw + " - num queries: " + queryCounter + " num bindings: " + bindingCounter + "  avg qps: " + avgQueriesPerSecond);
        }

    }



}
