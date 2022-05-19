package org.apache.jena.sparql.service;

public interface ServiceBatchRequest<I, G> {
	G getGroupKey();
	Batch<I> getBatch();
}