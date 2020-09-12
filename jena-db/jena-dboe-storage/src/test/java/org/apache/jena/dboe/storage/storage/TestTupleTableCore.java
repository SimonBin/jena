package org.apache.jena.dboe.storage.storage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.dboe.storage.quad.QuadTableCore;
import org.apache.jena.dboe.storage.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Assert;
import org.junit.Test;


public class TestTupleTableCore {
    @Test
    public void test1() {
        QuadTableCore table = new QuadTableCoreFromMapOfTripleTableCore(TripleTableCoreFromNestedMapsImpl::new);
        table.add(SSE.parseQuad("(:g1 :g1s1 :g1p1 :g1o1)"));
        table.add(SSE.parseQuad("(:g1 :g1s2 :g1p2 :g1o2)"));
        table.add(SSE.parseQuad("(:g2 :g2s1 :g2p1 :g2o1)"));
        table.add(SSE.parseQuad("(:g2 :g2s2 :g2p2 :g2o2)"));

        // lr = lookup result
        List<Node> lr = table.newFinder().projectOnly(3).distinct().stream().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(SSE.parseNode(":g1"), SSE.parseNode(":g2")), lr);
    }

}
