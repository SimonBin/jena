package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNodeWithCodec;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorArrayOfInts;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TripleStorages;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodecDictionary;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.StageBuilder;

public class MainEngineTest {

    public static void main(String[] args) throws IOException {
        String datasetFile = "/home/raven/research/jena-vs-tentris/data/swdf/swdf.nt";
//        String datasetFile = "/home/raven/research/jena-vs-tentris/data/watdiv/watdiv-15.nt";
//        String datasetFile = "/home/raven/research/jena-vs-tentris/data/watdiv/watdiv-100.nt";

        String queriesFile = "/home/raven/research/jena-vs-tentris/data/swdf/SWDF-Queries.txt";
//        String queriesFile = "/home/raven/research/jena-vs-tentris/data/watdiv/WatDiv-Queries.txt";


//        Collection<String> workloads = Files.readAllLines(
//                Paths.get(queriesFile))
//                .stream().skip(3).collect(Collectors.toList());
//
        Collection<String> workloads = Arrays.asList("SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }");


        init(0, datasetFile, workloads);
    }

    public static void init(int mode, String filename, Iterable<String> workloads) throws IOException {

//        if (args.length != 3) {
//            System.out.println("Requires 3 arguments: [MODE] [FILE-OR-ENDPOINT] [WORKLOAD]");
//            System.out.println("Modes: 0 = 0 = hyper trie, 1 = default model, 2 = tentris (http endpoint)");
//            System.exit(1);
//        }

        Model[] model = {null};
        Function<Query, QueryExecution> queryExecutor = null;

        // 0 = hyper trie, 1 = default model, 2 = tentris
        //int mode = 0;


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
                         .bufferBindings(false)
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
        case 3:
            // watdiv-15 Time until completion of a full run: 108.1 ms - num queries: 45 num bindings: 0  avg qps: 416.1240178294161
            queryExecutor = query -> QueryExecutionFactory.createServiceRequest("http://localhost:8895/sparql", query);
            break;
        default:
            throw new RuntimeException("no mode with this id");
        }


        if (model[0] != null) {
            Stopwatch loadingSw = Stopwatch.createStarted();
            System.out.println("Loading data " + filename);
            RDFDataMgr.read(model[0], filename);
            System.out.println("Loaded in " + loadingSw);
        }

//        Iterable<String> workloads = Arrays.asList(
//        		"SELECT DISTINCT ?b ?d ?e WHERE { ?a a ?b . ?c a ?d . ?a ?e ?c . }");


        for (int i = 0; i < 1; ++i) {
            doWork(queryExecutor, workloads);
        }
    }



    public static void doWork(Function<Query, QueryExecution> queryExecutor, Iterable<String> workloads) throws IOException {


        for (int j = 0; j < 100; ++j) {
            Stopwatch runTimeSw = Stopwatch.createStarted();

            int queryCounter = 0;
            long bindingCounter = 0;
            for(String queryStr : workloads) {
                ++queryCounter;

//                System.out.println(queryStr);
                Query query = QueryFactory.create(queryStr);

                Stopwatch executionTimeSw = Stopwatch.createStarted();
                try (QueryExecution qe = queryExecutor.apply(query)) {

                    ResultSet rs = qe.execSelect();

                    boolean reparseResultSet = false;
                    if (reparseResultSet) {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ResultSetMgr.write(baos, rs, ResultSetLang.SPARQLResultSetJSON);
                        rs = ResultSetMgr.read(new ByteArrayInputStream(baos.toByteArray()), ResultSetLang.SPARQLResultSetJSON);
                    }

//                    long count = ResultSetFormatter.consume(rs);

                    // ResultSetFormatter.consume materializes all Model objects which
                    // may take significantly more time than just listing the bindings
                    long count = 0;
                    while (rs.hasNext()) {
                        Binding b = rs.nextBinding();
//                        System.out.println(b);
                        ++count;
//                        break;
                    }

                    bindingCounter += count;
                    long elapsed = executionTimeSw.elapsed(TimeUnit.MILLISECONDS);
                    System.out.println("Execution time: " + elapsed + " - result set size: " + count);

                    if (elapsed > 500) {
                        System.out.println("  SLOW: " + queryStr);
                    }
                }
            }

            double avgQueriesPerSecond = queryCounter / (double)runTimeSw.elapsed(TimeUnit.NANOSECONDS) * 1000000000.0;
            System.out.println("Time until completion of a full run: " + runTimeSw + " - num queries: " + queryCounter + " num bindings: " + bindingCounter + "  avg qps: " + avgQueriesPerSecond);
        }

    }



    public static Model createHyperTrieBackedModel() {
        TupleAccessor<int[], Integer> backendAccessor = new TupleAccessorArrayOfInts(3);

//        StorageNodeMutable<int[], Integer, ?> storage =
//                TripleStorages.createHyperTrieStorageInt(backendAccessor);

        StorageNodeMutable<int[], Integer, ?> storage =
            TripleStorages.createHyperTrieStorage(backendAccessor);

        TupleCodec<Triple, Node, int[], Integer> tupleCodec
            = TupleCodecDictionary.createForInts(TupleAccessorTripleAnyToNull.INSTANCE, backendAccessor);

        TripleTableCore tripleTableCore =
                TripleTableFromStorageNodeWithCodec.create(tupleCodec, storage);

//      StorageNodeMutable<Triple, Node, ?> storage = createConventionalStorage();

//        StorageNodeMutable<Triple, Node, ?> wrapper = StorageComposers.wrapWithDictionary(
//                storage, TupleAccessorTripleAnyToNull.INSTANCE);

//        StorageNodeMutable<Triple, Node, ?> storage =
//                TripleStorages.createHyperTrieStorage(TupleAccessorTripleAnyToNull.INSTANCE);

//        TripleTableFromStorageNode<?> tripleTableCore = TripleTableFromStorageNode.create(storage);

        Model result = ModelFactory.createModelForGraph(
              GraphFromTripleTableCore.create(tripleTableCore));

        return result;
    }


}
