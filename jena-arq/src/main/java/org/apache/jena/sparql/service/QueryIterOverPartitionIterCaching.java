package org.apache.jena.sparql.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * QueryIterator wrapper that updates a cache based on partition events
 *
 * @author raven
 *
 */
public class QueryIterOverPartitionIterCaching
	extends QueryIterPartitionBase
{
	protected ExecutionContext execCxt;
	protected SimpleServiceCache cache;
	protected Op op;

	protected RefFuture<ServiceCacheValue> currentCacheEntryRef;
	protected ServiceCacheValue currentCacheEntry;

	// Function to map a binding to the offset in the cache
	protected Function<Binding, Long> bindingToOffset;

	protected Iterator<Binding> activeBatch = Collections.<Binding>emptyList().iterator();

	public QueryIterOverPartitionIterCaching(
			SimpleServiceCache cache,
			Op op,
			PartitionIterator partitionIterator, ExecutionContext execCxt) {
		super(partitionIterator);
		this.execCxt = execCxt;
	}

	@Override
	protected Binding moveToNext() {
		// Check if we have to take the next batch of events from the inptu iterator

		while (!activeBatch.hasNext()) {
			activeBatch = prepareNextBatch().iterator();
		}

		Binding result = activeBatch.hasNext() ? activeBatch.next() : null;
		return result;
	}

	/**
	 * (1) Consume a batch of elements from the partition iterator
	 * (2) Construct the bindings and update the caches accordingly
	 * (3) Return the batch of constructed bindings
	 */
	protected List<Binding> prepareNextBatch() {

		Binding result = null;
		int n = 50;

		// List<Binding> batch = new ArrayList<>();
		Batch<Binding> batch = new Batch


		long cacheBatchOffset = -1;

		Binding[] cacheBatch = new Binding[n];

		while (partitionIterator.hasNext()) {
			PartitionElt elt = partitionIterator.next();

			if (elt.isStart()) {
				PartitionStart evt = elt.asStart();

				cacheBatchOffset = -1;

				// ServiceCacheKey cacheKey = new ServiceCacheKey(evt., op, result);

				// submit the pending items to the cache
				// evt


			} else if (elt.isItem()) {
				Binding outputBinding = elt.asItem().getOutputBinding();

				long offset = bindingToOffset.apply(outputBinding);

				// If the item offset is not consecutive to the prior one then
				// submit what we have to the cache
				if (offset != cacheBatchOffset + 1) {
					// cache.getCache().claimIfPresent(null);
				}

			} else {
				throw new RuntimeException("Should never come here");
			}
		}

		return batch;
	}
}
