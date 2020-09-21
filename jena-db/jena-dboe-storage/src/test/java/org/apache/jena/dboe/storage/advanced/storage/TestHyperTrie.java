package org.apache.jena.dboe.storage.advanced.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.jena.dboe.storage.advanced.tuple.engine.MainEngineTest;
import org.junit.Test;

public class TestHyperTrie {

    @Test
    public void test() throws IOException {
        Iterable<String> workloads = Arrays.asList(
                "PREFIX : <http://www.example.org/>\n" +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX dbr: <http://dbpedia.org/resource/>\n" +
                "SELECT ?f {\n" +
                "  :e1 foaf:knows ?f .\n" +
                "  ?f  foaf:knows ?u .\n" +
                "  ?u  a          dbr:Unicorn\n" +
                "}");

        MainEngineTest.init(0, "tentris-example.ttl", workloads);
    }
}
