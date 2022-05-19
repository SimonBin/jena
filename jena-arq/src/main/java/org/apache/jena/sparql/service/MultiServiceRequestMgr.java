package org.apache.jena.sparql.service;

import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.locks.Lock;

import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceAccessor;
import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.ext.com.google.common.collect.RangeSet;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import com.esotericsoftware.kryo.NotNull;
import com.esotericsoftware.minlog.Log;


public class MultiServiceRequestMgr
// implements some iterator
{
	protected OpServiceInfo serviceInfo;
	protected Deque<Binding> pendingInputs;

	protected SimpleServiceCache cache = new SimpleServiceCache();


	// Output iterators indexed by PartitionIterator.getNextOutputId

	// Iterators are removed from this map upon consumption
	protected NavigableMap<Long, PartitionIterator> nextOutputIdToIterator;


	protected Map<Long, PartitionRequest> outputIdToRequest;

	protected long nextInputId;
	protected long nextOutputId;


	protected Map<Node, SingleServiceRequestMgr> requestMgr;


	protected QueryIterator activeIterator;


	/**  Ensure that at least there are active requests to serve the next n input bindings */
	protected int fetchAhead = 5;

	/** Allow reading at most this number of items ahead for the input iterator */
	protected int readAhead = 300;

	protected int maxBulkSize = 30;
	protected int maxRequestSize = 2000;


	public MultiServiceRequestMgr(OpServiceInfo serviceInfo) {
		this.serviceInfo = serviceInfo;
	}



	/**
	 * Schedule another input binding.
	 *
	 * Duplicate input bindings in the same batch:
	 * If that input binding's ouput is (partially) cached with unknown total size then subsequent bindings will
	 * NOT rely on the cache because it is not known which amount can be cached (in the worst case we might run out of memory).
	 *
	 * If that input binding has a known size below a threshold then the cache will be used.
	 *
	 */
	protected void scheduleInput(Binding partition) {
		Node substServiceNode = serviceInfo.getSubstServiceNode(partition);

		ServiceCacheKey cacheKey = new ServiceCacheKey(substServiceNode, serviceInfo.getRawQueryOp(), partition);

		RefFuture<ServiceCacheValue> cacheValueRef = cache.getCache().claim(cacheKey);
		ServiceCacheValue serviceCacheValue = cacheValueRef.await();



		// TODO Check if there is any scheduled request that would add more ranges to data


		Query rawQuery = serviceInfo.getRawQuery();
		Range<Long> range = RangeUtils.toRange(serviceInfo.getOffset(), serviceInfo.getLimit());
		RangeSet<Long> fetchRanges = serviceCacheValue.getFetchRanges(range);

		SingleServiceRequestMgr mgr = requestMgr.computeIfAbsent(substServiceNode, k -> new SingleServiceRequestMgr());


		Slice<Binding[]> slice = serviceCacheValue.getSlice();
		Lock lock = slice.getReadWriteLock().readLock();

		SliceAccessor<Binding[]> accessor = slice.newSliceAccessor();

		RangeSet<Long> loaded;
		long knownSize;
		try {
			loaded = slice.getLoadedRanges();
			knownSize = slice.getKnownSize();

			accessor.claimByOffsetRange(0, Long.MAX_VALUE);
		} finally {
			lock.unlock();
		}


		// Iterate the present/absent ranges
		for (Range<Long> range : fetchRanges) {
			long start = range.lowerEndpoint();
			long end = range.hasLowerBound() ? range.upperEndpoint() : Long.MAX_VALUE;

			boolean isLoaded = false;

			// accessor.r


			PartitionRequest request = new PartitionRequest(partition, nextOutputId, start, end);
			mgr.add(request);

			++nextOutputId;
		}
	}



	// Service -> Partition (Binding) -> Range
	// protected Table<Node, >


//	public void addInput(Binding partition, long start, long end) {
//
//		long outputId = nextOutputId;
//		Node substServiceNode = serviceInfo.getSubstServiceNode(partition);
//
//
//		PartitionRequest request = new PartitionRequest(partition, outputId, start, end);
//		ServiceRequestMgr mgr = requestMgr.computeIfAbsent(substServiceNode, k -> new ServiceRequestMgr());
//
//		mgr.add(request);
//
//		outputIdToRequest.put(outputId, request);
//	}
//

	public PartitionRequest getRequest(long outputId) {
		return null;
	}

	public Binding nextOutput() {
		Binding result;
		if (!activeIterator.hasNext()) {
			activeIterator.close();

			// Remove the prior iterator
			nextOutputIdToIterator.remove(nextOutputId);
			++nextOutputId;

			Entry<Long, PartitionIterator> e = nextOutputIdToIterator.firstEntry();
			PartitionIterator pit = e.getValue();
			activeIterator = pit;

			// Find the request that supplies bindings with the given outputId
			result = null;

		} else {
			result = activeIterator.next();
		}

		return result;
	}

	public void close() {
		for (PartitionIterator pit : nextOutputIdToIterator.values()) {
			try {
				pit.close();
			} catch (Exception e) {
				 Log.info("Failed to close an iterator", e);
			}
		}
	}
}
