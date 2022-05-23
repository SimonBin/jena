package org.apache.jena.sparql.service;

import org.aksw.commons.io.buffer.array.ArrayOps;
import org.aksw.commons.io.buffer.plain.BufferWithPages;
import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceInMemory;
import org.aksw.commons.util.range.RangeUtils;
import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.ext.com.google.common.collect.RangeSet;
import org.apache.jena.sparql.engine.binding.Binding;


public class ServiceCacheValue {

    protected ArrayOps<Binding[]> arrayOps = ArrayOps.createFor(Binding.class);

    // Some slice construction
	protected Slice<Binding[]> slice = SliceInMemory.create(arrayOps, BufferWithPages.create(arrayOps, 10000));


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