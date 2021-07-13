package org.apache.jena.dboe.storage.advanced.tuple.api;

import java.util.List;

public abstract class GenericTupleAccessorFromListOfKeysBase<D, C, K>
	implements GenericTupleAccessor<D, C, K>
{
	protected List<K> keys;

	public GenericTupleAccessorFromListOfKeysBase(List<K> keys) {
		super();
		this.keys = keys;
	}

	@Override
	public int getDimension() {
		return keys.size();
	}

	@Override
	public K keyAtOrdinal(int index) {
		return keys.get(index);
	}
	
	@Override
	public C get(D tupleLike, int componentIdx) {
		K key = keys.get(componentIdx);
		C result = get(tupleLike, key);
		return result;
	}
}
