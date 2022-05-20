package org.apache.jena.sparql.service;

public interface ServiceBatchRequest<G, I> {
	G getGroupKey();
	Batch<I> getBatch();
}