package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.stream.Stream;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.binding.Binding;

public interface AdvancedTripleFind {
    Stream<Binding> find(boolean distinct, Node s, Node p, Node o);
}
