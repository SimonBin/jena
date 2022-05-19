package org.apache.jena.sparql.service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator.OfInt;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.jena.ext.com.google.common.collect.AbstractIterator;

class RequestScheduler<I, G> {

	/**  Ensure that at least there are active requests to serve the next n input bindings */
	protected int fetchAhead = 5;

	/** Allow reading at most this number of items ahead for the input iterator to completely fill
	 *  the batch request for the next response */
	protected int maxReadAhead = 300;

	protected int maxBulkSize = 30;
	protected int maxRequestSize = 2000;

	/** Do not group inputs into the same batch if their ids are this (or more) of that amount apart */
	protected int maxInputDistance = 50;

	// protected Iterator<I> inputIterator;
	protected Function<I, G> inputToGroup;

	public RequestScheduler(Function<I, G> inputToGroup, int maxBulkSize) {
		super();
		this.inputToGroup = inputToGroup;
		this.maxBulkSize = maxBulkSize;
	}


	public Iterator<ServiceBatchRequest<I, G>> group(Iterator<I> inputIterator) {
		return new Grouper(inputIterator);
	}

	class Grouper
		extends AbstractIterator<ServiceBatchRequest<I, G>>
	{
		protected Iterator<I> inputIterator;

		/** The position of the inputIterator */
		protected long inputIteratorOffset;


		/** The offset of the next item being returned */
		protected long nextResultOffset;


		protected long nextInputId;

		// the outer navigable map has to lowest offset of the batch
		protected Map<G, NavigableMap<Long, Batch<I>>> groupToBatches = new HashMap<>();

		// Offsets of the group keys
		protected NavigableMap<Long, G> nextGroup = new TreeMap<>();

		public Grouper(Iterator<I> inputIterator) {
			this(inputIterator, 0);
		}

		public Grouper(Iterator<I> inputIterator, long inputIteratorOffset) {
			super();
			this.inputIterator = inputIterator;
			this.inputIteratorOffset = inputIteratorOffset;
			this.nextResultOffset = inputIteratorOffset;
		}

		@Override
		protected ServiceBatchRequest<I, G> computeNext() {
			G resultGroupKey = Optional.ofNullable(nextGroup.firstEntry()).map(Entry::getValue).orElse(null);

			G lastGroupKey = null;

			// Cached references
			NavigableMap<Long, Batch<I>> batches = null;
			Batch<I> batch = null;

			while (inputIterator.hasNext() && inputIteratorOffset - nextResultOffset < maxReadAhead) {
				I input = inputIterator.next();
				G groupKey = inputToGroup.apply(input);

				if (!Objects.equals(groupKey, lastGroupKey)) {
					lastGroupKey = groupKey;

					if (resultGroupKey == null) {
						resultGroupKey = groupKey;
					}

					batches = groupToBatches.computeIfAbsent(groupKey, x -> new TreeMap<>());
					if (batches.isEmpty()) {
						batch = new Batch<>();
						batches.put(inputIteratorOffset, batch);
						nextGroup.put(inputIteratorOffset, groupKey);
					} else {
						batch = batches.lastEntry().getValue();
					}
				}

				// Check whether we need to start a new request
				// Either because the batch is full or the differences between the input ids is too great
				long batchEndOffset = batch.getNextValidIndex();
				long distance = nextInputId - batchEndOffset;

				// If the item is consecutive add it to the list
				int batchSize = batch.size();
				if (distance > maxInputDistance || batchSize >= maxBulkSize) {
					batch = new Batch<>();
					batches.put(inputIteratorOffset, batch);
				}
				batch.put(inputIteratorOffset, input);
				++inputIteratorOffset;

				// If the batch of the result group just became then break
				if (groupKey.equals(resultGroupKey) && batchSize + 1 >= maxBulkSize) {
					break;
				}
			}

			// Return and remove the first batch from our data structures

			ServiceBatchRequest<I, G> result;
			Iterator<Entry<Long, G>> nextGroupIt = nextGroup.entrySet().iterator();
			if (nextGroupIt.hasNext()) {
				Entry<Long, G> e = nextGroupIt.next();
				resultGroupKey = e.getValue();
				nextGroupIt.remove();
				nextInputId = e.getKey();

				NavigableMap<Long, Batch<I>> nextBatches = groupToBatches.get(resultGroupKey);
				Iterator<Batch<I>> nextBatchesIt = nextBatches.values().iterator();
				Batch<I> resultBatch = nextBatchesIt.next();
				nextBatchesIt.remove();


				result = new ServiceBatchRequestImpl<>(resultGroupKey, resultBatch);
			} else {
				result = endOfData();
			}
			return result;
		}
	}


	public static void main(String[] args) {
		OfInt individualIt = IntStream.range(0, 10).iterator();


		RequestScheduler<Integer, String> scheduler = new RequestScheduler<>(v -> "group" + (v % 3), 2);
		Iterator<ServiceBatchRequest<Integer, String>> batchIt = scheduler.group(individualIt);

		while (batchIt.hasNext()) {
			System.out.println(batchIt.next());
		}


	}




}