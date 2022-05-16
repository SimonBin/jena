/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.service.enhancer.claimingcache.AsyncClaimingCache;
import org.apache.jena.sparql.service.enhancer.claimingcache.AsyncClaimingCacheImpl;
import org.apache.jena.sparql.service.enhancer.claimingcache.RefFuture;
import org.apache.jena.sparql.service.enhancer.init.InitServiceEnhancer;
import org.apache.jena.sparql.util.Context;

import com.github.benmanes.caffeine.cache.Caffeine;

public class ServiceResponseCache {
    // service / op / joinVars / binding / idx
    protected AsyncClaimingCache<ServiceCacheKey, ServiceCacheValue> cache;

    protected AtomicLong entryCounter = new AtomicLong(0l);

    /** Secondary index over cache keys */
    protected Map<Long, ServiceCacheKey> idToKey = new ConcurrentHashMap<>();

    public ServiceResponseCache() {
        //super();
        AsyncClaimingCacheImpl.Builder<ServiceCacheKey, ServiceCacheValue> builder =
                AsyncClaimingCacheImpl.newBuilder(Caffeine.newBuilder().maximumSize(300));
        builder = builder
                .setCacheLoader(key -> {
                    long id = entryCounter.getAndIncrement();
                    idToKey.put(id, key);
                    ServiceCacheValue r = new ServiceCacheValue(id);
                    Log.debug(ServiceResponseCache.class, "Loaded cache entry: " + id);
                    return r;
                })
                .setAtomicRemovalListener((k, v, c) -> {
                    // We are not yet handling cancellation of loading a key; in that case the value may not yet be available
                    // Handle it here here with null for v?
                    if (v != null) {
                        long id = v.getId();
                        Log.debug(ServiceResponseCache.class, "Removed cache entry: " + id);
                        idToKey.remove(id);
                    }
                });
        cache = builder.build();
    }

    public AsyncClaimingCache<ServiceCacheKey, ServiceCacheValue> getCache() {
        return cache;
    }

    public RefFuture<ServiceCacheValue> claim(ServiceCacheKey key) {
        return cache.claim(key);
    }

    public Map<Long, ServiceCacheKey> getIdToKey() {
        return idToKey;
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    /** Return the global instance (if any) in ARQ.getContex() */
    public static ServiceResponseCache get() {
        return get(ARQ.getContext());
    }

    public static ServiceResponseCache get(Context cxt) {
        return cxt.get(InitServiceEnhancer.serviceCache);
    }

    public static void set(Context cxt, ServiceResponseCache cache) {
        cxt.put(InitServiceEnhancer.serviceCache, cache);
    }
}
