package org.apache.jena.sparql.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

/** A set of possibly disconnected ranges of items.
 * Consecutive items are managed in lists */
// No longer used - use BatchFlat
public class BatchWithList<T> {
	protected NavigableMap<Long, List<T>> itemRanges;
	protected int size;

	// Note: The contained lists should be considered immutable
	protected NavigableMap<Long, List<T>> unmodifiableItemRanges;

	public BatchWithList() {
		super();
		this.itemRanges = new TreeMap<>();
		this.unmodifiableItemRanges = Collections.unmodifiableNavigableMap(itemRanges);
		this.size = 0;
	}

	// Items must be added with ascending indexes
	// Adding an item with a lower index than already seen raises an IllegalArgumentException
	public void put(long index, T item) {
		Entry<Long, List<T>> e = itemRanges.lastEntry();
		long lastRangeStart = e == null ? -1 : e.getKey();
		List<T> items = e == null ? null : e.getValue();
		long nextOffset;

		if (e == null || index > (nextOffset = lastRangeStart + items.size())) {
			items = new ArrayList<>();
			itemRanges.put(index, items);
		} else if (index != nextOffset) {
			throw new IllegalArgumentException("Index is lower than an existing one");
		}

		items.add(item);

		++size;
	}

	public long getNextValidIndex() {
		long result;
		if (isEmpty()) {
			result = 0;
		} else {
			Entry<Long, List<T>> e = itemRanges.lastEntry();
			result = e.getKey() + e.getValue().size();
		}
		return result;
	}

	public NavigableMap<Long, List<T>> getItemRanges() {
		return unmodifiableItemRanges;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return "Batch [size=" + size + ", itemRanges=" + itemRanges + "]";
	}


}