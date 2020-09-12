package org.apache.jena.dboe.storage.storage;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.dboe.storage.quad.QuadTableCore;
import org.apache.jena.dboe.storage.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Test;

public class TestTupleTableCore {
    @Test
    public void test1() {
        QuadTableCore table = new QuadTableCoreFromMapOfTripleTableCore(TripleTableCoreFromNestedMapsImpl::new);
        table.add(SSE.parseQuad("(:g1 :s1 :p1 :o1"));
        table.add(SSE.parseQuad("(:g2 :s2 :p2 :o2"));

        List<Node> lr = table.newFinder().projectOnly(0).distinct().stream().collect(Collectors.toList());
        System.out.println(lr);
    }

}
