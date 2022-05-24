package org.apache.jena.sparql.service;

import java.util.Collection;
import java.util.Iterator;

public class CollectionUtils {
	public static <T, C extends Collection<T>> C addAll(C out, Iterator<T> it) {
		while (it.hasNext()) {
			T item = it.next();
			out.add(item);
		}
		return out;
	}
}
