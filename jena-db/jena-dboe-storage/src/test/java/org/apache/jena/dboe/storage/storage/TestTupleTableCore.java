package org.apache.jena.dboe.storage.storage;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2NodeCompound;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Assert;
import org.junit.Test;


public class TestTupleTableCore {
    @Test
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
        TupleAccessor<Quad, Node> accessor = new TupleAccessorQuad();

        // Hooray - ugly complex nested type expression - exactly what I would have wanted for Sparqlify's
        // source selection index like 8 years ago - but back then I constructed a similar nested expression starting from the root
        // This time I do it bottom-up and it is so much better!
        Meta2NodeCompound<Quad, Node, Map<Node, Map<Node, Map<Node, Map<Node, Quad>>>>> storage =
            StorageComposers.innerMap(3, LinkedHashMap::new,
                StorageComposers.innerMap(0, LinkedHashMap::new,
                        StorageComposers.innerMap(1, LinkedHashMap::new,
                            StorageComposers.leafMap(2, accessor, LinkedHashMap::new))));

        System.out.println("Storage structure: " + storage);
        Map<Node, Map<Node, Map<Node, Map<Node, Quad>>>> rootTyped = storage.newStore();
        Object root = rootTyped;

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
}
