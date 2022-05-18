package org.apache.jena.sparql.service;

import java.util.NavigableSet;

import org.apache.jena.ext.com.google.common.cache.Cache;
import org.apache.jena.ext.com.google.common.cache.CacheBuilder;
import org.apache.jena.sparql.engine.binding.Binding;

public class SimpleServiceCache {
	public static class Value {
		protected NavigableSet<Binding> idx;
		protected long knownSize;
	}


	// service / op / joinVars / binding / idx
	protected Cache<ServiceCacheKey, Value> cache;


	public SimpleServiceCache() {
		super();
		this.cache = CacheBuilder.newBuilder().build();
	}

}
