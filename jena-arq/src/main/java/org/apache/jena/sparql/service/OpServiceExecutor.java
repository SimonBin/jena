package org.apache.jena.sparql.service;

import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.QueryIterator;

@FunctionalInterface
public interface OpServiceExecutor {
	QueryIterator exec(OpService opService);
}
