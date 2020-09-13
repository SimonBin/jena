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

        // Hooray - ugly complex nested type expression
        Meta2NodeCompound<Quad, Node, Map<Node, Map<Node, Map<Node, Map<Node, Quad>>>>> storage =
            StorageComposers.innerMap(3, LinkedHashMap::new,
                StorageComposers.innerMap(0, LinkedHashMap::new,
                        StorageComposers.innerMap(1, LinkedHashMap::new,
                            StorageComposers.leafMap(2, accessor, LinkedHashMap::new))));

        System.out.println("Storage structure: " + storage);
        Object root = storage.newStore();

        Quad q1 = SSE.parseQuad("(:g1 :s1 :g1p1 :g1o1)");
        Quad q2 = SSE.parseQuad("(:g1 :s1 :g1p2 :g1o2)");
        Quad q3 = SSE.parseQuad("(:g2 :s2 :g2p1 :g2o1)");
        Quad q4 = SSE.parseQuad("(:g2 :s2 :g2p2 :g2o2)");

        storage.add(root, q1);
        storage.add(root, q2);
        storage.add(root, q3);
        storage.add(root, q4);


        //storage.add(root, )

    }
}
