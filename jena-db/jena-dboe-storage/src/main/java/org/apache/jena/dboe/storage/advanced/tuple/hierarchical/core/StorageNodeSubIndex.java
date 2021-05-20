package org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core;


/**
 * A wrapper for a storage node that can indirectly index key tuples.
 * 
 * Remaps the key components of an input tuple to a different tuple whose components can be indexed
 * using the StorageNode machinery.
 * 
 * This can be used for RDF star indexing: Components of the input tuples may be triples which can be
 * treated as tuples again.
 * 
 * 
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 * @param <X>
 */
public class StorageNodeSubIndex<D, C, V, X extends StorageNodeMutable<D,C,V>>
	extends StorageNodeMutableForwarding<D, C, V, X>
{
	protected 

	@Override
	protected X getDelegate() {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public boolean add(V store, D tupleLike) {
		// TODO Auto-generated method stub
		return super.add(store, tupleLike);
	}
}
