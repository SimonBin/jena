package org.apache.jena.sparql.service;

import java.util.NavigableMap;

interface Batch<T> {

	NavigableMap<Long, T> getItems();
	void put(long index, T item);
	long getNextValidIndex();
	boolean isEmpty();
	int size();
}
