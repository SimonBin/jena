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

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.service.enhancer.slice.api.ArrayOps;
import org.apache.jena.sparql.service.enhancer.slice.api.Slice;
import org.apache.jena.sparql.service.enhancer.slice.impl.SliceInMemoryCache;


public class ServiceCacheValue {

    protected long id;

    // Some slice construction
    protected Slice<Binding[]> slice;

    public ServiceCacheValue(long id) {
        this(id, SliceInMemoryCache.create(ArrayOps.createFor(Binding.class), 10000, 15));
    }

    public ServiceCacheValue(long id, Slice<Binding[]> slice) {
        super();
        this.id = id;
        this.slice = slice;
    }

    public long getId() {
        return id;
    }

    public Slice<Binding[]> getSlice() {
        return slice;
    }

    /** Get the ranges which need to be fetched from the backend for the given request range */
//	public RangeSet<Long> getFetchRanges(Range<Long> requestRange) {
//		RangeSet<Long> gaps = RangeUtils.gaps(requestRange, slice.getGaps(requestRange));
//
//		// TODO Move some of the request scheduling from AdvancedRangeCache to RangeUtils:
//		// (a) skipOverAvailableDataThreshold (do not make new requests if there are only few items between gaps)
//		// (b) makeRequestSizeThreshold (schedule multiple requests if a gap is too large)
//		// well, (b) has to be resolved at a completely different layer anyway, and (a) should be a post processing of this method's
//		// return value
//
//		return gaps;
//	}
}