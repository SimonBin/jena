package org.apache.jena.dboe.storage.storage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.quad.QuadTableCore;
import org.apache.jena.dboe.storage.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.triple.TripleTableCoreFromNestedMapsImpl;
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

}
