package org.apache.jena.sparql.service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import org.aksw.commons.collections.CloseableIterator;
import org.aksw.commons.io.input.ReadableChannel;
import org.aksw.commons.io.input.ReadableChannelWithLimit;
import org.aksw.commons.io.input.ReadableChannels;
import org.aksw.commons.io.slice.ReadableChannelOverSliceAccessor;
import org.aksw.commons.io.slice.Slice;
import org.aksw.commons.io.slice.SliceAccessor;
import org.aksw.commons.util.closeable.AutoCloseables;
import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.ext.com.google.common.collect.TreeBasedTable;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingProject;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.service.BatchQueryRewriter.BatchQueryRewriteResult;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.math.LongMath;


/** Bridge between jena's and aksw-commons iterator */
class ClosableIteratorAdapter
	extends QueryIterPlainWrapper
{
	protected CloseableIterator<Binding> delegate;

	public ClosableIteratorAdapter(CloseableIterator<Binding> delegate) {
		super(delegate);
		this.delegate = delegate;
	}

	@Override
	protected void closeIterator() {
		AutoCloseables.close(((AutoCloseable)delegate));
	}
}

public class RequestExecutor
	extends QueryIterSlottedBase<Binding>
{

	protected OpServiceInfo serviceInfo;

	/**  Ensure that at least there are active requests to serve the next n input bindings */
	protected int fetchAhead = 5;
	protected int maxRequestSize = 2000;


	protected OpServiceExecutorImpl opExecutor;
	protected Iterator<ServiceBatchRequest<Node, Binding>> batchIterator;
	protected SimpleServiceCache cache;

	protected Var idxVar;
	// The index of the next allocation
	// protected long nextOutputId = 0;


	// The current read marker - never greater than the allocation
	// protected PartitionKey currentPartitionKey;


	// Result iteration
	protected long currentInputId = -1;
	protected long currentRangeId = -1;
	protected QueryIterPeek activeIter;


	// Request allocation
	protected long nextAllocOutputId = 0;
	protected long nextAllocInputId = 0;

	protected Map<Long, Binding> inputToBinding = new HashMap<>();
	protected TreeBasedTable<Long, Long, Long> inputToRangeToOutput = TreeBasedTable.create();
	protected HashMap<Long, PartitionKey> outputToPartKey = new HashMap<>();
	protected Map<Long, QueryIterPeek> outputToIter = new HashMap<>();

	// Map for when an iterator can be closed
	protected Map<Long, QueryIterPeek> outputToClose = new HashMap<>();

	// protected TreeBasedTable<Long, Long, QueryIterPeek> inputIdToRangeIdToIterator = TreeBasedTable.create();
	// protected NavigableMap<PartitionKey, QueryIterPeek> partitionKeyToIterator;

	// Map an iterator to the last idx it can supply - once this index is passed the iterator can be closed
	// protected Multimap<PartitionKey, QueryIterPeek> closeByIdx = ArrayListMultimap.create();
	// protected Table<Long, Long, QueryIterPeek> closeByPartKey = HashBasedTable.create();


	public RequestExecutor(
			OpServiceExecutorImpl opExector,
			OpServiceInfo serviceInfo,
			SimpleServiceCache cache,
			Iterator<ServiceBatchRequest<Node, Binding>> batchIterator) {
		this.opExecutor = opExector;
		this.serviceInfo = serviceInfo;
		this.batchIterator = batchIterator;
		this.cache = cache;

		this.idxVar = Var.alloc("__idx__");
		ExecutionContext execCxt = opExector.getExecCxt();
		this.activeIter = QueryIterPeek.create(QueryIterPlainWrapper.create(Collections.<Binding>emptyList().iterator(), execCxt), execCxt);
	}


//	public static long getIndex(Binding b, Var idxVar) {
//		int idx;
//		if (bulkSize > 1) {
//			Node idxNode = rawChild.get(idxVar);
//			Object obj = idxNode.getLiteralValue();
//			if (!(obj instanceof Number)) {
//				throw new ExprEvalException("Index was not returned as a number");
//			}
//			idx = ((Number)obj).intValue();
//
//			if (idx < 0 || idx > bulkSize) {
//				throw new QueryExecException("Returned index out of range");
//			}
//		} else {
//			idx = 0;
//		}
//	}

	public static Number getNumberNullable(Binding binding, Var var) {
		Node node = binding.get(var);
		Number result = null;
		if (node != null) {
			Object obj = node.getLiteralValue();
			if (!(obj instanceof Number)) {
				throw new ExprEvalException("Value is not returned as a number");
			}
			result = ((Number)obj);
		}

		return result;
	}

	public static Number getNumber(Binding binding, Var var) {
		return Objects.requireNonNull(getNumberNullable(binding, var), "Number must not be null");
	}

	public static long getLong(Binding binding, Var var) {
		return getNumber(binding, var).longValue();
	}

	@Override
	protected Binding moveToNext() {

		Binding parentBinding = null;
		Binding childBinding = null;

		// Peek the next binding on the active iterator and verify that it maps to the current
		// partition key
		while (true) {
			if (activeIter.hasNext()) {
				Binding peek = activeIter.peek();
				long peekOutputId = getLong(peek, idxVar);
				PartitionKey partKey = outputToPartKey.get(peekOutputId);

				boolean matchesCurrentPartition = partKey.getInputId() == currentInputId &&
						partKey.getRangeId() == currentRangeId;

				if (matchesCurrentPartition) {
					parentBinding = inputToBinding.get(currentInputId);
					childBinding = activeIter.next();
					break;
				}
			}

			// Cleanup of no longer needed resources
			Long outputId = inputToRangeToOutput.get(currentInputId, currentRangeId);
			QueryIterPeek toClose = outputToClose.get(outputId);
			if (toClose != null) {
				toClose.close();
				outputToClose.remove(outputId);
			}
			inputToRangeToOutput.remove(currentInputId, currentRangeId);
			outputToIter.remove(outputId);

			// Increment rangeId/inputId until we reach the end
			++currentRangeId;
			SortedMap<Long, Long> row = inputToRangeToOutput.row(currentInputId);
			if (!row.containsKey(currentRangeId)) {
				inputToBinding.remove(currentInputId);
				++currentInputId;
				currentRangeId = 0;
			}

			// Check if we need to lead the next batch
			if (!inputToRangeToOutput.containsRow(currentInputId)) {
				if (batchIterator.hasNext()) {
					execNextBatch();
				}
			}

			// If there is still no further batch then we assume we reached the end
			if (!inputToRangeToOutput.containsRow(currentInputId)) {
				break;
			}

			outputId = inputToRangeToOutput.get(currentInputId, currentRangeId);
			activeIter = outputToIter.get(outputId);
		}

		Binding result = childBinding == null
				? null
				: BindingFactory.builder(parentBinding).addAll(childBinding).build();

		return result;
	}


	public static <T, C extends Collection<T>> C addAll(C out, Iterator<T> it) {
		while (it.hasNext()) {
			T item = it.next();
			out.add(item);
		}
		return out;
	}

	public static <C extends Collection<Var>> C varsMentioned(C out, Iterator<Binding> it) {
		while (it.hasNext()) {
			Binding b = it.next();
			addAll(out, b.vars());
		}
		return out;
	}

	public static Set<Var> varsMentioned(Iterable<Binding> bindings) {
		Set<Var> result = new LinkedHashSet<>();
		return varsMentioned(result, bindings.iterator());
	}



	/** Execute the next batch and register all iterators with {@link #nextOutputIdToIterator} */
	// seqId = sequential number injected into the request
	// inputId = id (index) of the input binding
	// rangeId = id of the range w.r.t. to the input binding
	// partitionKey = (inputId, rangeId)
	public void execNextBatch() {

		ServiceBatchRequest<Node, Binding> batchRequest = batchIterator.next();
		Node substServiceNode = batchRequest.getGroupKey();


		// Refine the request w.r.t. the cache
		Batch<Binding> batch = batchRequest.getBatch();
		Set<Var> varsMentioned = varsMentioned(batch.getItems().values());

		Set<Var> joinVars = Sets.intersection(serviceInfo.getServiceVars(), varsMentioned);



		// Iterator<Binding> itBindings = batch.getItems().values().iterator();// .stream().flatMap(List::stream).iterator();

		NavigableMap<Long, Binding> batchItems = batch.getItems();


		BatchQueryRewriter rewriter = new BatchQueryRewriter(serviceInfo, idxVar);


		// List<PartitionRequest<Binding>> backendRequests = new ArrayList<>();
		Batch<PartitionRequest<Binding>> backendRequests = new BatchFlat<>();

		inputToBinding.putAll(batchItems);

		// long nextOutputId;
		for (Entry<Long, Binding> e : batchItems.entrySet()) {
		// while (itBindings.hasNext()) {

			long inputId = e.getKey();
			Binding inputBinding = e.getValue();// itBindings.next();
			Binding joinBinding = new BindingProject(joinVars, inputBinding);

			ServiceCacheKey cacheKey = new ServiceCacheKey(substServiceNode, serviceInfo.getRawQueryOp(), joinBinding);
			System.out.println("Lookup with cache key " + cacheKey);


			// TODO Elegantly handle case where cache is null

			RefFuture<ServiceCacheValue> cacheValueRef = cache.getCache().claim(cacheKey);
			ServiceCacheValue serviceCacheValue = cacheValueRef.await();

			// Lock an existing cache entry so we can read out the loaded ranges
			Slice<Binding[]> slice = serviceCacheValue.getSlice();
			Lock lock = slice.getReadWriteLock().readLock();
			lock.lock();

			RangeSet<Long> loadedRanges;
			long knownSize;
			try {
				loadedRanges = slice.getLoadedRanges();
				knownSize = slice.getKnownSize();

				// Iterate the present/absent ranges
				long start = serviceInfo.getOffset();
				if (start == Query.NOLIMIT) {
					start = 0;
				}

				long limit = serviceInfo.getLimit();

				long max = knownSize < 0 ? Long.MAX_VALUE : knownSize;
				long end = limit == Query.NOLIMIT ? max : LongMath.saturatedAdd(start, limit);
				end = Math.min(end, max);

				Range<Long> initialRange = knownSize < 0
					? Range.atLeast(start)
					: Range.closedOpen(start, end);

				RangeSet<Long> missingRanges = loadedRanges.complement().subRangeSet(initialRange);

				RangeMap<Long, Boolean> allRanges = TreeRangeMap.create();
				loadedRanges.asRanges().forEach(r -> allRanges.put(r, true));
				missingRanges.asRanges().forEach(r -> allRanges.put(r, false));


				long rangeId = 0;
				for (Entry<Range<Long>, Boolean> f : allRanges.asMapOfRanges().entrySet()) {
					// PartitionKey partitionKey = new PartitionKey(inputId, rangeId);

					Range<Long> range = f.getKey();
					boolean isLoaded = f.getValue();

					long lo = range.lowerEndpoint();
					long hi = range.hasUpperBound() ? range.upperEndpoint() : Long.MAX_VALUE;

					if (isLoaded) {
						SliceAccessor<Binding[]> accessor = slice.newSliceAccessor();
						ReadableChannel<Binding[]> channel =
								new ReadableChannelWithLimit<>(
										new ReadableChannelOverSliceAccessor<>(accessor, lo),
										hi);

						CloseableIterator<Binding> baseIt = ReadableChannels.newIterator(channel);
						// TODO Wrap as QueryIterPeek
						QueryIterPeek it = QueryIterPeek.create(new ClosableIteratorAdapter(baseIt), opExecutor.getExecCxt());
						//partitionKeyToIterator.put(partitionKey, it);

						outputToIter.put(nextAllocOutputId, it);
						outputToClose.put(nextAllocOutputId, it);
					} else {
						PartitionRequest<Binding> request = new PartitionRequest<>(nextAllocOutputId, inputBinding, lo, hi);
						backendRequests.put(nextAllocOutputId, request);
					}

					inputToRangeToOutput.put(inputId, rangeId, nextAllocOutputId);
					outputToPartKey.put(nextAllocOutputId, new PartitionKey(nextAllocInputId, rangeId));

					++rangeId;
					++nextAllocOutputId;
				}
			} finally {
				lock.unlock();
			}
			++nextAllocInputId;
		}

		// Create a remote execution if needed
		if (!backendRequests.isEmpty()) {
			BatchQueryRewriteResult rewrite = rewriter.rewrite(backendRequests);
			System.out.println(rewrite);

	        Op newSubOp = rewrite.getOp();
	        OpService substitutedOp = new OpService(substServiceNode, newSubOp, serviceInfo.getOpService().getSilent());


	        // Execute the batch request and wrap it such that ...
	        // (1) we can merge it with other backend and cache requests in the right order
	        // (2) responses are written to the cache
	        QueryIterator qIter = opExecutor.exec(substitutedOp);

	        // Wrap the interator such that the items are cached
	        if (cache != null) {
	        	qIter = new QueryIterWrapperCache(qIter, 128, cache, joinVars, backendRequests, idxVar, substServiceNode, serviceInfo.getRawQueryOp());
	        }


	        // Wrap the query iter such that we can peek the next binding in order
	        // to decide from which iterator to take the next element
	        QueryIterPeek iter = QueryIterPeek.create(qIter, opExecutor.getExecCxt());

	        for (long offset : batch.getItems().keySet()) {
	        	outputToIter.put(offset, iter);
	        }
	        outputToClose.put(batch.getItems().lastKey(), iter);
		}
	}



}

