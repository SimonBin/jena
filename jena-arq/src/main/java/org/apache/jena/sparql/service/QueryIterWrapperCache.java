package org.apache.jena.sparql.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceAccessor;
import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;


public class QueryIterWrapperCache
	extends QueryIterWrapperBulk
{
	protected SimpleServiceCache cache;
	protected Batch<PartitionRequest<Binding>> inputBatch;
	protected Op op; // The operation that was executed
	protected Var idxVar; // CacheKeyAccessor cacheKeyAccessor;


	protected long prevInputIdx = -1;
	protected PartitionRequest<Binding> inputPart; // Value stored here for debugging
	protected long currentOffset = 0;

	/** The claimed cache entry - prevents premature eviction */
	protected RefFuture<ServiceCacheValue> claimedCacheEntry = null;

	/** The accessor for writing data to the cache */
	protected SliceAccessor<Binding[]> cacheDataAccessor = null;

	public QueryIterWrapperCache(
			QueryIterator qIter, int batchSize,
			SimpleServiceCache cache,
			Batch<PartitionRequest<Binding>> inputBatch,
			Op op) {
		super(qIter, batchSize);
		this.inputBatch = inputBatch;
	}

	@Override
	protected void onBatch(List<Binding> output) {

		NavigableMap<Long, PartitionRequest<Binding>> inputs = inputBatch.getItems();
		Iterator<Binding> it = output.iterator();


		Binding[] arr = new Binding[output.size()];
		int arrLen = 0;

		for (int i = 0; it.hasNext(); ++i) {
			Binding outputBinding = it.next();

			long inputIdx = RequestExecutor.getLong(null, idxVar);
			PartitionRequest<Binding> inputPart = inputs.get(inputIdx);

			if (inputIdx != prevInputIdx) {
				if (prevInputIdx != -1) {
					inputPart = inputs.get(inputIdx);
					// Submit batch so far
					long start = inputPart.getOffset() + currentOffset;
					long end = start + arrLen;

					closeCurrentCacheResources();

					ServiceCacheKey cacheKey = null;
					claimedCacheEntry = cache.getCache().claim(cacheKey);
					ServiceCacheValue c = claimedCacheEntry.await();

					Slice<Binding[]> slice = c.getSlice();
					cacheDataAccessor = slice.newSliceAccessor();
					cacheDataAccessor.claimByOffsetRange(start, end);

					try {
						cacheDataAccessor.write(start, arr, 0, arrLen);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

				arrLen = 0;
				prevInputIdx = inputIdx;
				currentOffset = 0;
			}

			arr[arrLen++] = outputBinding;
		}

		super.onBatch(output);
	}

	protected void closeCurrentCacheResources() {
		if (cacheDataAccessor != null) {
			cacheDataAccessor.close();
		}

		if (claimedCacheEntry != null) {
			claimedCacheEntry.close();
		}
	}

	@Override
	protected void closeIterator() {
		closeCurrentCacheResources();

		super.closeIterator();
	}
}
