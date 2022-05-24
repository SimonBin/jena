package org.apache.jena.sparql.service.to_verify;

import org.apache.jena.sparql.service.QueryIterSlottedBase;

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