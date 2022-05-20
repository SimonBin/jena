package org.apache.jena.sparql.service;

public class PartitionRequest<I> {
	protected long outputId;
	// protected T target; // where to send to request to
	protected I partition;
	protected long offset;
	protected long limit;

	public PartitionRequest(
			long outputId,
			// T target,
			I partition,
			long offset,
			long limit) {
		super();
		this.outputId = outputId;
		this.partition = partition;
		this.offset = offset;
		this.limit = limit;
	}

	public long getOutputId() {
		return outputId;
	}

//	public T getTarget() {
//		return target;
//	}

	public I getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}

	public long getLimit() {
		return limit;
	}
}
