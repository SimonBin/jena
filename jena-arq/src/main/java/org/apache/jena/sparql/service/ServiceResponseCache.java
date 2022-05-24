package org.apache.jena.sparql.service;

import org.aksw.commons.cache.async.AsyncClaimingCache;
import org.aksw.commons.cache.async.AsyncClaimingCacheImpl;
import org.aksw.commons.util.ref.RefFuture;

import com.github.benmanes.caffeine.cache.Caffeine;

public class ServiceResponseCache {
	// service / op / joinVars / binding / idx
	protected AsyncClaimingCache<ServiceCacheKey, ServiceCacheValue> cache;


	public ServiceResponseCache() {
		//super();
		AsyncClaimingCacheImpl.Builder<ServiceCacheKey, ServiceCacheValue> builder =
				AsyncClaimingCacheImpl.newBuilder(Caffeine.newBuilder());
		builder = builder.setCacheLoader(key -> new ServiceCacheValue());
		cache = builder.build();
	}


	public AsyncClaimingCache<ServiceCacheKey, ServiceCacheValue> getCache() {
		return cache;
	}

	public RefFuture<ServiceCacheValue> claim(ServiceCacheKey key) {
		return cache.claim(key);
	}
}
