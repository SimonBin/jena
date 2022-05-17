package org.apache.jena.sparql.service;

import org.apache.jena.sparql.engine.QueryIterator;

/**
 * "Finishes" an intermediate {@link PartitionIterator} into a {@link QueryIterator}
 * The main purpose of the finisher is to allow for caching of responses
 * on the level of the individual input (parent) bindings.
 */
public interface Finisher {
	public QueryIterator finish(PartitionIterator partIt);
}