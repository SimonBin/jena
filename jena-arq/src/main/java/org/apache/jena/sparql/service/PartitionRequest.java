package org.apache.jena.sparql.service;

public class PartitionRequest<I>
{
	protected long outputId;
	protected I partition;
	protected long offset;
	protected long limit;

	public PartitionRequest(
			long outputId,
			I partition,
			long offset,
			long limit) {
		super();
		this.outputId = outputId;
		// this.rangeId = rangeId;
		this.partition = partition;
		this.offset = offset;
		this.limit = limit;
	}

	public long getOutputId() {
		return outputId;
	}

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
