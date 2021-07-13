package org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core;


/**
 * A decorator for a storage node that can indirectly index key tuples.
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
public class StorageNodeSubIndex<D1, D2, C1, C2 V, K>
	//extends StorageNodeMutableForwarding<D, C, V, X>
{
	protected StorageNode<D1, C1, V> primary;
	protected StorageNode<D2, C2, V> secondary;

	protected Function<D1, D2> inputToSecondary;
	
		
	@Override
	public boolean add(V store, D tupleLike) {
		// TODO Auto-generated method stub
		return super.add(store, tupleLike);
	}
}
