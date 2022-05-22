package org.apache.jena.sparql.service;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

public class BatchFlat<T>
	implements Batch<T>
{
	protected NavigableMap<Long, T> items;

	// Note: The contained lists should be considered immutable
	protected NavigableMap<Long, T> unmodifiableItems;

	public BatchFlat() {
		super();
		this.items = new TreeMap<>();
		this.unmodifiableItems = Collections.unmodifiableNavigableMap(items);
	}

	// Items must be added with ascending indexes
	// Adding an item with a lower index than already seen raises an IllegalArgumentException
	public void put(long index, T item) {
		long nextValidIndex = getNextValidIndex();
		if (index < nextValidIndex) {
			throw new IllegalArgumentException("Index is lower than an existing one");
		}

		items.put(index, item);
	}

	public long getNextValidIndex() {
		long result = items.isEmpty()
				? 0
				: items.lastKey() + 1;
		return result;
	}

	public NavigableMap<Long, T> getItems() {
		return unmodifiableItems;
	}

	public boolean isEmpty() {
		return items.isEmpty();
	}

	public int size() {
		return items.size();
	}

	@Override
	public String toString() {
		return "Batch [size=" + size() + ", itemRanges=" + items + "]";
	}
}
