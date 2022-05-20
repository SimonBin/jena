package org.apache.jena.sparql.service;

// Execution of the returned request is guaranteed to cover at least the next fetchAhead inputs
// Usually it will cover more than that; when getting close to the getFirstMissId()
// the driver may decide to send a further request
class ServiceBatchRequestImpl<G, I>
	implements ServiceBatchRequest<G, I>
{
	protected G groupKey;
	protected Batch<I> batch;
	// protected NavigableMap<Long, List<I>> schedule;

	public ServiceBatchRequestImpl(G groupKey, Batch<I> batch) {
		super();
		this.groupKey = groupKey;
		this.batch = batch;
	}

	@Override
	public G getGroupKey() {
		return groupKey;
	}

	@Override
	public Batch<I> getBatch() {
		return batch;
	}

	@Override
	public String toString() {
		return "ServiceBatchRequestImpl [groupKey=" + groupKey + ", batch=" + batch + "]";
	}
}