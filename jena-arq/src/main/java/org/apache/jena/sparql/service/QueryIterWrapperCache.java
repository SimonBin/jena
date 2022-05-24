package org.apache.jena.sparql.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceAccessor;
import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingProject;


public class QueryIterWrapperCache
	extends QueryIterWrapperBulk
{
	protected ServiceResponseCache cache;
	protected Batch<PartitionRequest<Binding>> inputBatch;
	protected Op op; // The operation that was executed
	protected Var idxVar; // CacheKeyAccessor cacheKeyAccessor;
	protected Node serviceNode;

	protected Set<Var> joinVars;
	protected long prevInputIdx = -1;
	protected PartitionRequest<Binding> inputPart; // Value stored here for debugging
	protected long currentOffset = 0;

	/** The claimed cache entry - prevents premature eviction */
	protected RefFuture<ServiceCacheValue> claimedCacheEntry = null;

	/** The accessor for writing data to the cache */
	protected SliceAccessor<Binding[]> cacheDataAccessor = null;

	public QueryIterWrapperCache(
			QueryIterator qIter,
			int batchSize,
			ServiceResponseCache cache,
			Set<Var> joinVars,
			Batch<PartitionRequest<Binding>> inputBatch,
			Var idxVar,
			Node serviceNode,
			Op op) {
		super(qIter, batchSize);
		this.cache = cache;
		this.joinVars = joinVars;
		this.inputBatch = inputBatch;
		this.idxVar = idxVar;
		this.serviceNode = serviceNode;
		this.op = op;
	}

	@Override
	protected void onBatch(List<Binding> output) {

		NavigableMap<Long, PartitionRequest<Binding>> inputs = inputBatch.getItems();
		Iterator<Binding> it = output.iterator();

		Binding[] arr = new Binding[output.size()];
		int arrLen = 0;

		// Collect consecutive binding that refer to the same inputIdx into an array
		// and then write that array into the cache
		// The assumption is that inputIdx is monotonous - this should be guaranteed by the query construction
		// whose execution yields these bindings

		long inputIdx = 0;
		// while (inputIdx != Long.MAX_VALUE) {
		while (true) {
			Binding outputBinding = it.hasNext() ? it.next() : null;

			inputIdx = outputBinding == null
					? Long.MAX_VALUE
					: RequestExecutor.getLong(outputBinding, idxVar);
			// PartitionRequest<Binding> inputPart = inputs.get(inputIdx);

			if (inputIdx != prevInputIdx) {
				if (prevInputIdx != -1) {
					inputPart = inputs.get(prevInputIdx);
					// Submit batch so far
					Binding input = inputPart.getPartition();
					long start = inputPart.getOffset() + currentOffset;
					long end = start + arrLen;


					closeCurrentCacheResources();

					Binding joinBinding = new BindingProject(joinVars, input);

					ServiceCacheKey cacheKey = new ServiceCacheKey(serviceNode, op, joinBinding);

					// System.out.println("Writing to cache key " + cacheKey);

					claimedCacheEntry = cache.getCache().claim(cacheKey);
					ServiceCacheValue c = claimedCacheEntry.await();

					Slice<Binding[]> slice = c.getSlice();
					cacheDataAccessor = slice.newSliceAccessor();
					cacheDataAccessor.claimByOffsetRange(start, end);

					try {
						cacheDataAccessor.write(start, arr, 0, arrLen);

						if (!inputPart.hasLimit() && (output.isEmpty() || inputIdx != Long.MAX_VALUE)) {
							slice.setKnownSize(end);
						}

					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

				arrLen = 0;
				if (inputIdx != Long.MAX_VALUE) {
					prevInputIdx = inputIdx;
					currentOffset = 0;
				}
			}

			if (inputIdx == Long.MAX_VALUE) {
				break;
			}


			// Hide the idxVar of binding being written to the cache
			Set<Var> visibleVars = BindingUtils.varsMentioned(outputBinding);
			visibleVars.remove(idxVar);

			arr[arrLen++] = new BindingProject(visibleVars, outputBinding);
		}

		super.onBatch(output);
	}

	protected void closeCurrentCacheResources() {
		if (cacheDataAccessor != null) {
			cacheDataAccessor.close();
			cacheDataAccessor = null;
		}

		if (claimedCacheEntry != null) {
			claimedCacheEntry.close();
			claimedCacheEntry = null;
		}
	}

	@Override
	protected void closeIterator() {
		closeCurrentCacheResources();

		super.closeIterator();
	}
}
