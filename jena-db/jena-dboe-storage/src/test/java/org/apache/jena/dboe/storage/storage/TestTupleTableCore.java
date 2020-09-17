package org.apache.jena.dboe.storage.storage;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.altN;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafSet;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuadAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQueryImpl;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.KeyReducer;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.KeyReducerTuple;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.NodeStats;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessorImpl;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.TupleQueryAnalyzer;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2NodeCompound;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerBinder;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Assert;
import org.junit.Test;

import com.github.jsonldjava.shaded.com.google.common.collect.Maps;


public class TestTupleTableCore {
//    @Test
    public void test1() {
        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        QuadTableCore table = new QuadTableCoreFromMapOfTripleTableCore(TripleTableCoreFromNestedMapsImpl::new);
        table.add(q1);
        table.add(q2);
        table.add(q3);
        table.add(q4);

        Node g1 = q1.getGraph();
        Node s1 = q1.getSubject();
        Node g2 = q3.getGraph();
        Node s2 = q3.getSubject();

        // lr = lookup result
        List<Quad> lr0a = table.newFinder().stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(q1, q2, q3, q4), lr0a);


        List<Quad> lr0b = table.newFinder().eq(0, s1).stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(q1, q2), lr0b);

        List<Node> lr1 = table.newFinder().projectOnly(3).distinct().stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(g1, g2), lr1);

        List<Tuple<Node>> lr2 = table.newFinder().project(3, 0).stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(
                TupleFactory.create2(g1, s1),
                TupleFactory.create2(g1, s1),
                TupleFactory.create2(g2, s2),
                TupleFactory.create2(g2, s2)),
                lr2);

        List<Tuple<Node>> lr3 = table.newFinder().project(3, 0).distinct().stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(
                TupleFactory.create2(g1, s1),
                TupleFactory.create2(g2, s2)),
                lr3);
    }

    @Test
    public void test2() {

        // Hooray - ugly complex nested type expression - exactly what I would have wanted for Sparqlify's
        // source selection index like 8 years ago - but back then I constructed a similar nested expression starting from the root
        // This time I do it bottom-up and it is so much better!
        Meta2NodeCompound<Quad, Node, Entry<Map<Node, Entry<Map<Node, Map<Node, Map<Node, Quad>>>, Set<Quad>>>, Set<Quad>>> storage =
            alt2(
                innerMap(3, LinkedHashMap::new,
                    alt2(
                        innerMap(0, LinkedHashMap::new,
                            innerMap(1, LinkedHashMap::new,
                                leafMap(2, TupleAccessorQuad.INSTANCE, LinkedHashMap::new))),
                        leafSet(TupleAccessorQuad.INSTANCE, LinkedHashSet::new))),
                leafSet(TupleAccessorQuad.INSTANCE, LinkedHashSet::new));


        System.out.println("Storage structure: " + storage);
        Entry<Map<Node, Entry<Map<Node, Map<Node, Map<Node, Quad>>>, Set<Quad>>>, Set<Quad>> root = storage.newStore();

        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        System.out.println("Performing inserts");
        storage.add(root, q1);
        storage.add(root, q2);
        storage.add(root, q3);
        storage.add(root, q4);

        StoreAccessor<Quad, Node> rootAccessor = StoreAccessorImpl.createForStore(storage);



        TupleQuery<Node> tupleQuery = new TupleQueryImpl<>(4);
        tupleQuery.setDistinct(true);
        tupleQuery.setConstraint(0, q1.getSubject());
        // tupleQuery.setConstraint(3, RDF.Nodes.type);
        tupleQuery.setProject(1);



        System.out.println("BEGIN OF REPORTS");
        NodeStats<Quad, Node> bestMatch = TupleQueryAnalyzer.analyze(
                tupleQuery,
                rootAccessor,
                new int[] {10, 10, 1, 100});
        System.out.println("Best match: " + bestMatch);

        ResultStreamerBinder<Quad, Node, Tuple<Node>> rs = TupleQueryAnalyzer.createResultStreamer(
                bestMatch,
                tupleQuery,
                TupleAccessorQuadAnyToNull.INSTANCE);

        rs.bind(root).streamAsComponent()
            .forEach(tuple -> System.out.println("GOT TUPLE: " + tuple));

        System.out.println("END OF REPORTS =====================");

        if (true) {
            return;
        }


        KeyReducer<Entry<?, ?>> toPairs = (p, i, k) -> Maps.immutableEntry(p, k);

//        rootAccessor.child(0).cartesianProduct(
//                Quad.create(Node.ANY, Node.ANY, Node.ANY, Node.ANY),
//                TupleAccessorQuadAnyToNull.INSTANCE,
//                null,
//                toPairs)
//        .streamRaw(root).forEach(x -> System.out.println("CARTPROD0: " + x.getKey()));
//
//        rootAccessor.child(0).child(0).child(0).cartesianProduct(
//                Quad.create(Node.ANY, Node.ANY, Node.ANY, Node.ANY),
//                TupleAccessorQuadAnyToNull.INSTANCE,
//                null,
//                toPairs)
//            .streamRaw(root).forEach(x -> System.out.println("CARTPROD1: " + x.getKey()));
//

        rootAccessor.child(0).child(0).child(0).child(0).child(0).streamerForContent(
                Quad.create(Node.ANY, q1.getSubject(), Node.ANY, Node.ANY),
                TupleAccessorQuadAnyToNull.INSTANCE)
        .streamRaw(root).forEach(x -> System.out.println("CONTENT2: " + x));

        rootAccessor.leastNestedChildOrSelf().streamerForContent(
                Quad.create(Node.ANY, Node.ANY, Node.ANY, Node.ANY),
                TupleAccessorQuadAnyToNull.INSTANCE)
            .streamRaw(root).forEach(x -> System.out.println("CONTENT: " + x));

        rootAccessor.child(0).child(0).child(0).child(0).child(0).cartesianProduct(
                Quad.create(Node.ANY, q1.getSubject(), Node.ANY, Node.ANY),
                TupleAccessorQuadAnyToNull.INSTANCE,
                null,
                toPairs)
        .streamRaw(root).forEach(x -> System.out.println("CARTPROD2: " + x.getKey()));




        StoreAccessor<Quad, Node> oAccessor = rootAccessor.child(0).child(0).child(0).child(0).child(0);
        KeyReducerTuple<Node> fancyKeyReducer = KeyReducerTuple.createForProjection(oAccessor, new int[] {0, 0, 3, 2, 0});

        oAccessor.cartesianProduct(
                Quad.create(Node.ANY, Node.ANY, Node.ANY, q4.getObject()),
                TupleAccessorQuadAnyToNull.INSTANCE,
                fancyKeyReducer.newAccumulator(),
                fancyKeyReducer)
        .streamRaw(root).map(Entry::getKey).map(fancyKeyReducer::makeTuple)
        .forEach(x -> System.out.println("CARTPROD3: " + x));

        //forEach(x -> System.out.println("CARTPROD3: " + x.getKey()));

        System.out.println("Baked: " + rootAccessor);

        storage.getChildren().get(0)
            .streamerForKeysAsComponent(Quad.create(Node.ANY, Node.ANY, Node.ANY, Node.ANY), TupleAccessorQuadAnyToNull.INSTANCE)
            .streamRaw(root.getKey()).forEach(x -> System.out.println("KEY: " + x));




//        StoreAccessor<Quad, Node> oAccessor = rootAccessor.child(0).child(0).child(0).child(0).child(0);
//        KeyReducerTuple<Node> fancyKeyReducer = KeyReducerTuple.createForProjection(oAccessor, new int[] {0, 0, 3, 2, 0});
//
//        oAccessor.cartesianProduct(
//                Quad.create(Node.ANY, Node.ANY, Node.ANY, q4.getObject()),
//                TupleAccessorQuadAnyToNull.INSTANCE,
//                fancyKeyReducer.newAccumulator(),
//                fancyKeyReducer)
//        .streamRaw(root).map(Entry::getKey).map(fancyKeyReducer::makeTuple)
//        .forEach(x -> System.out.println("CARTPROD3: " + x));
//        bestMatch.getAccessor().cartesianProduct(pattern, accessor, initialAccumulator, keyReducer)



        List<?> entries1 = storage.streamEntries(root).collect(Collectors.toList());
        for(Object entry : entries1) {
            System.out.println(entry);
        }

        System.out.println("Performing removals");
        storage.remove(root, q1);
        storage.remove(root, q2);
        List<?> entries2 = storage.streamEntries(root).collect(Collectors.toList());
        for(Object entry : entries2) {
            System.out.println(entry);
        }

/*
Output:

Storage structure: ([3] -> ([0] -> ([1] -> ([2]))))
Performing inserts
http://example/g1=http://example/s1=http://example/g1p1=http://example/g1o1=[http://example/g1 http://example/s1 http://example/g1p1 http://example/g1o1]
http://example/g1=http://example/s1=http://example/g1p2=http://example/g1o2=[http://example/g1 http://example/s1 http://example/g1p2 http://example/g1o2]
http://example/g2=http://example/s2=http://example/g2p1=http://example/g2o1=[http://example/g2 http://example/s2 http://example/g2p1 http://example/g2o1]
http://example/g2=http://example/s2=http://example/g2p2=http://example/g2o2=[http://example/g2 http://example/s2 http://example/g2p2 http://example/g2o2]

Performing removals
http://example/g2=http://example/s2=http://example/g2p1=http://example/g2o1=[http://example/g2 http://example/s2 http://example/g2p1 http://example/g2o1]
http://example/g2=http://example/s2=http://example/g2p2=http://example/g2o2=[http://example/g2 http://example/s2 http://example/g2p2 http://example/g2o2]

*/


    }


//    @Test
    public void test3() {
        TupleAccessor<Quad, Node> accessor = new TupleAccessorQuad();

        Meta2NodeCompound<Quad, Node, Map<Node, Set<Quad>>> storage =
                innerMap(3, LinkedHashMap::new,
                        leafSet(accessor, LinkedHashSet::new));

        System.out.println("Storage structure: " + storage);
        Map<Node, Set<Quad>> root = storage.newStore();

        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        System.out.println("Performing inserts");
        storage.add(root, q1);
        storage.add(root, q2);
        storage.add(root, q3);
        storage.add(root, q4);

        List<?> entries1 = storage.streamEntries(root).collect(Collectors.toList());
        for(Object entry : entries1) {
            System.out.println(entry);
        }

        System.out.println("Performing removals");
        storage.remove(root, q1);
        storage.remove(root, q2);
        List<?> entries2 = storage.streamEntries(root).collect(Collectors.toList());
        for(Object entry : entries2) {
            System.out.println(entry);
        }
    }

//    @Test
    public void testAlternatives2() {
        TupleAccessor<Quad, Node> accessor = new TupleAccessorQuad();

        Meta2NodeCompound<Quad, Node, Entry<Map<Node, Set<Quad>>, Map<Node, Set<Quad>>>> storage =
                alt2(
                    innerMap(3, LinkedHashMap::new,
                            leafSet(accessor, LinkedHashSet::new)),
                    innerMap(0, LinkedHashMap::new,
                            leafSet(accessor, LinkedHashSet::new)));

        Entry<Map<Node, Set<Quad>>, Map<Node, Set<Quad>>> store = storage.newStore();
    }

//    @Test
    public void testAlternativesN() {
        TupleAccessor<Quad, Node> accessor = new TupleAccessorQuad();

        Meta2NodeCompound<Quad, Node, ?> storage = altN(Arrays.asList(
                innerMap(3, LinkedHashMap::new,
                        leafSet(accessor, LinkedHashSet::new)),
                innerMap(0, LinkedHashMap::new,
                        leafSet(accessor, LinkedHashSet::new))));


        System.out.println("Storage structure: " + storage);
        Object root = storage.newStore();

        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        System.out.println("Performing inserts");
        storage.addRaw(root, q1);
        storage.addRaw(root, q2);
        storage.addRaw(root, q3);
        storage.addRaw(root, q4);

        List<?> entries1 = storage.streamEntriesRaw(root).collect(Collectors.toList());
        for(Object entry : entries1) {
            System.out.println(entry);
        }

        System.out.println("Performing removals");
        storage.removeRaw(root, q1);
        storage.removeRaw(root, q2);
        List<?> entries2 = storage.streamEntriesRaw(root).collect(Collectors.toList());
        for(Object entry : entries2) {
            System.out.println(entry);
        }
    }
}
