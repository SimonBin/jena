package org.apache.jena.sparql.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;

/**
 * Wrapper that reads {@link #batchSize} items at once from the underlying query iterator
 * into a list. Then the {@link #onBatch(List)} handler is notified and finally the items
 * of the list are emitted. The last call to handler will always be with an empty list.
 *
 * Intended use case is to cache items in bulk which may avoid excessive synchronization on every
 * single item.
 */
public class QueryIterWrapperBulk
	extends QueryIterSlottedBase<Binding>
{
	protected Iterator<Binding> activeBatchIt = Collections.<Binding>emptyList().iterator();
	protected int batchSize;
	protected QueryIterator delegate;

	public QueryIterWrapperBulk(QueryIterator qIter, int batchSize) {
		super();
		this.batchSize = batchSize;
		this.delegate = qIter;
	}

	@Override
	protected Binding moveToNext() {
		Binding result;
		if (!activeBatchIt.hasNext()) {
			List<Binding> newBatch = new ArrayList<>(batchSize);

			for (int i = 0; i < batchSize && delegate.hasNext(); ++i) {
				Binding binding = delegate.next();
				newBatch.add(binding);
			}

			// Notify about the next batch
			onBatch(newBatch);
			activeBatchIt = newBatch.iterator();

			result = activeBatchIt.hasNext() ? activeBatchIt.next() : null;
		} else {
			result = activeBatchIt.next();
		}
		return result;
	}

	@Override
	protected void closeIterator() {
		delegate.close();
	}

	protected void onBatch(List<Binding> batch) {}


}
