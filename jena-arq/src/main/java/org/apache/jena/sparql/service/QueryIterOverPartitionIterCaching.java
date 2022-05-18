package org.apache.jena.sparql.service;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * The basic QueryIterator wrapper implementation over a partition iterator.
 * Only emits the bindings of partition item events.
 *
 * @author raven
 *
 */
public class QueryIterOverPartitionIterCaching
	extends QueryIterPartitionBase
{
	protected ExecutionContext execCxt;



	public QueryIterOverPartitionIterCaching(PartitionIterator partitionIterator, ExecutionContext execCxt) {
		super(partitionIterator);
		this.execCxt = execCxt;
	}

	@Override
	protected Binding moveToNext() {
		Binding result = null;
		while (partitionIterator.hasNext()) {
			PartitionElt elt = partitionIterator.next();
			if (elt.isItem()) {
				result = elt.asItem().getBinding();
				break;
			}
		}

		return result;
	}
}