package org.apache.jena.sparql.service;

/** Base class for building QueryIterators over PartitionIterators */
public abstract class QueryIterPartitionBase
	extends QueryIterSlottedBase<PartitionElt>
{
	protected PartitionIterator partitionIterator;

	public QueryIterPartitionBase(PartitionIterator partitionIterator) {
		super();
		this.partitionIterator = partitionIterator;
	}

	@Override
	public void closeIterator() {
		partitionIterator.close();
	}
}